/**
 * The MIT License
 * Copyright (c) 2010 Tad Glines
 *
 * Contributors: Ovea.com, Mycila.com
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package com.glines.socketio.server.transport.jetty;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.glines.socketio.server.*;
import org.eclipse.jetty.websocket.WebSocket;
import org.eclipse.jetty.websocket.WebSocketFactory;
import org.eclipse.jetty.websocket.WebSocketFactory.Acceptor;

public final class JettyWebSocketTransport extends AbstractTransport {

    private static final Logger LOGGER = Logger.getLogger(JettyWebSocketTransport.class.getName());

    private final WebSocketFactory wsFactory = new WebSocketFactory(new Acceptor() {
      @Override
      public WebSocket doWebSocketConnect(HttpServletRequest request, String protocol) {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean checkOrigin(HttpServletRequest arg0, String arg1) {
        throw new UnsupportedOperationException();
      }
    });

    @Override
    public void init() throws TransportInitializationException {
        wsFactory.setBufferSize(getConfig().getBufferSize());
        wsFactory.setMaxIdleTime(getConfig().getMaxIdle());

        if (LOGGER.isLoggable(Level.FINE))
            LOGGER.fine(getType() + " configuration:\n" +
                    " - bufferSize=" + wsFactory.getBufferSize() + "\n" +
                    " - maxIdle=" + wsFactory.getMaxIdleTime());
    }

    @Override
    public TransportType getType() {
        return TransportType.WEB_SOCKET;
    }

    @Override
    public void handle(HttpServletRequest request,
                       HttpServletResponse response,
                       Transport.InboundFactory inboundFactory,
                       SessionManager sessionFactory) throws IOException {

        String sessionId = null;
        String transport = null;

        String path = request.getPathInfo();
        if (path != null && path.length() > 0 && !"/".equals(path)) {
            if (path.startsWith("/")) path = path.substring(1);
            String[] parts = path.split("/");
            if (parts.length >= 3) {
                transport = parts[1] == null || parts[1].length() == 0 || parts[1].equals("null") ? null : parts[1];
                sessionId = parts[2] == null || parts[2].length() == 0 || parts[2].equals("null") ? null : parts[2];
            }
        }

        if ("GET".equals(request.getMethod()) && sessionId != null
                && (transport.equals("websocket") || transport.equals("flashsocket"))) {
            boolean hixie = request.getHeader("Sec-WebSocket-Key1") != null;

            String protocol = request.getHeader(hixie ? "Sec-WebSocket-Protocol" : "WebSocket-Protocol");
            if (protocol == null)
                protocol = request.getHeader("Sec-WebSocket-Protocol");

            String host = request.getHeader("Host");
            String origin = request.getHeader("Origin");
            if (origin == null) {
                origin = host;
            }

            SocketIOInbound inbound = inboundFactory.getInbound(request);
            if (inbound == null) {
                if (hixie) {
                    response.setHeader("Connection", "close");
                }
                response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
            } else {
                SocketIOSession session;
                TransportHandler handler;
                session = sessionFactory.getSession(sessionId);

                if (session == null) {
                    session = sessionFactory.createSession(inbound, sessionId);
                    handler = newHandler(WebSocket.class, session);
                    handler.init(getConfig());
                    //handler.onConnect();
                } else {
                    handler = session.getTransportHandler();
                }

                wsFactory.upgrade(request, response, WebSocket.class.cast(handler), protocol);
                handler.sendMessage(new SocketIOFrame(SocketIOFrame.FrameType.CONNECT, SocketIOFrame.TEXT_MESSAGE_TYPE, ""));
                handler.onConnect();
            }
        } else {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, this + " transport error: Invalid request");
        }
    }

}
