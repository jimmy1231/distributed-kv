package testing;

import app_kvECS.TCPSockModule;
import shared.messages.UnifiedMessage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class MockServer extends Thread {
    private static String host = "localhost";
    private static int port = 50001;

    private ServerSocket serverSocket;

    MockServer() {
        /* Default constructor */
    }

    private class ConnReceptor extends Thread {
        InputStream input;
        OutputStream output;
        private Socket socket;
        boolean running;

        ConnReceptor(Socket socket) {
            this.socket = socket;
            running = true;
        }

        @Override
        public void run() {
            System.out.printf("CONNECTION RECEIVED: %s:%d\n",
                socket.getLocalAddress(),
                socket.getLocalPort());
            try {
                input = socket.getInputStream();
                output = socket.getOutputStream();

                /* Send ACK to client */
                TCPSockModule.send(output, "Connection established!");
            } catch (IOException e) {
                return;
            }

            UnifiedMessage request;
            String msg;
            while (running) {
                /*
                 * Tries to read the underlying socket via input
                 * stream, if peer has closed connection, recv will
                 * return NULL. We check this and close connection
                 * if this ever happens.
                 */
                System.out.println("POLLING FOR MESSAGE");
                msg = TCPSockModule.recv(input);
                System.out.println("MSG: " + msg);
                if (Objects.isNull(msg)) {
                    running = false;
                }

                if (Objects.nonNull(msg) && !msg.isEmpty()) {
                    request = new UnifiedMessage()
                        .deserialize(msg);

                    /* Handle request */
                    System.out.println("HANDLING REQUEST");

                    if (!TCPSockModule.send(output, request.serialize())) {
                        /* exit if failed to send */
                        running = false;
                    }
                }
            }

            System.out.printf("CONNECTION EXIT: %s:%d\n",
                socket.getLocalAddress(),
                socket.getLocalPort());
        }
    }

    @Override
    public void run() {
        try {
            serverSocket = new ServerSocket(port);
        } catch (IOException e) {
            System.out.println("Could not start server");
            return;
        }

        System.out.printf("Server listening on %s:%d\n",
            host, port);

        List<ConnReceptor> receptors = new ArrayList<>();
        while(true) {
            ConnReceptor receptor;
            Socket conn;
            try {
                /* Blocks until a connection is made */
                conn = serverSocket.accept();
                System.out.printf("CONNECTION: %s:%d\n",
                    conn.getLocalAddress().toString(),
                    conn.getLocalPort());

                /* Start new thread to handle this connection */
                receptor = new ConnReceptor(conn);
                receptor.start();
                receptors.add(receptor);
            } catch (IOException e) {
                /* Stop all connections */
                for (ConnReceptor r : receptors) {
                    r.running = false;
                }
                break;
            }
        }
    }

    public static void main(String[] args) {
        MockServer server = new MockServer();
        server.start();
    }
}
