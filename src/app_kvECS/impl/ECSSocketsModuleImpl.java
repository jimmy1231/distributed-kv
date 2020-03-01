package app_kvECS.impl;

import app_kvECS.ECSSocketsModule;
import app_kvECS.KVAdminRequest;
import app_kvECS.KVAdminResponse;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.Logger;
import sun.nio.ch.IOUtil;

import java.io.*;
import java.util.Objects;

public class ECSSocketsModuleImpl extends ECSSocketsModule {
    private static Logger logger = Logger.getLogger(ECSSocketsModuleImpl.class);
    private static int MAX_READ_BYTES = 1024;

    private InputStream input;
    private OutputStream output;
    private ObjectMapper objectMapper;

    public ECSSocketsModuleImpl(String host, int port) throws IOException {
        super(host, port);
        input = this.socket.getInputStream();
        output = this.socket.getOutputStream();
        objectMapper = new ObjectMapper();
        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    /**
     * Do request. Blocks until response is received.
     *
     * @param request
     */
    @Override
    public KVAdminResponse doRequest(KVAdminRequest request) throws Exception {
        KVAdminResponse response = null;

        /* Do request */
        byte[] requestBytes = request.toString().getBytes();
        output.write(requestBytes, 0, requestBytes.length);
        output.flush();

        /* Wait for response */
        while(true) {
            try {
                String responseStr = recv();
                if (Objects.nonNull(responseStr)) {
                    System.out.println(responseStr);
                    response = objectMapper.readValue(
                        responseStr, KVAdminResponse.class
                    );
                    break;
                }
            } catch (Exception e) {
                System.out.println("ERRORRRRR: " + e.toString());
                logger.error("ECS do request error", e);
                throw e;
            }
        }

        return response;
    }

    private String recv() {
        String response = null;

        BufferedInputStream bis;
        ByteArrayOutputStream bas;
        try {
            bis = new BufferedInputStream(input);
            bas = new ByteArrayOutputStream();
            byte[] buf = new byte[MAX_READ_BYTES];

            int len;
            while ((len = bis.read(buf)) > 0) {
                bas.write(buf, 0, len);
                break;
            }
            response = bas.toString("UTF-8");
        } catch (IOException e) {
            logger.error("Error reading input stream", e);
            response = null;
        }

        return response;

    }
}
