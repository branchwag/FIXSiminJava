import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

public class FIXClient {
    private static final String SERVER_HOST = "127.0.0.1";
    private static final int SERVER_PORT = 5000;
    private static final String SENDER_COMP_ID = "TESTINJAVA";
    private static final String TARGET_COMP_ID = "MINIFIX";
    private static final int HEARTBEAT_INTERVAL = 30; //seconds

    private Socket socket;
    private BufferedReader input;
    private PrintWriter output;
    private volatile boolean isRunning;
    private int messageSequenceNumber;
    private ScheduledExecutorService scheduler;

    public static void main(String[] args) {
        FIXClient client = new FIXClient();
        try {
            client.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void start() throws IOException {
        connect();
        isRunning = true;
        messageSequenceNumber = 1;

        // Initialize scheduler with two tasks
        scheduler = Executors.newScheduledThreadPool(2);
        
        // Schedule heartbeat sending
        scheduler.scheduleAtFixedRate(() -> {
            if (isRunning) {
                sendHeartbeat(null);  // Regular heartbeat without TestReqID
            }
        }, HEARTBEAT_INTERVAL, HEARTBEAT_INTERVAL, TimeUnit.SECONDS);
        
        // Schedule test request sending
        scheduler.scheduleAtFixedRate(this::sendTestRequest, 
            HEARTBEAT_INTERVAL/2, HEARTBEAT_INTERVAL/2, TimeUnit.SECONDS);

        sendLogonMessage();
        listenForMessages();
    }

    private void connect() throws IOException {
        socket = new Socket(SERVER_HOST, SERVER_PORT);
        socket.setKeepAlive(true);
        socket.setTcpNoDelay(true);
        System.out.println("Connected to FIX server at " + SERVER_HOST + ":" + SERVER_PORT);
    }

    private void sendLogonMessage() {
        String logonMessage = createLogonMessage();
        System.out.println("Sending logon message: " + logonMessage);
        sendMessage(logonMessage);
    }

    private synchronized void sendMessage(String message) {
        // Convert to ASCII and add SOH
        byte[] messageBytes = message.replaceAll("\\|", "\u0001").getBytes(StandardCharsets.US_ASCII);
        try {
            socket.getOutputStream().write(messageBytes);
            socket.getOutputStream().write('\u0001');
            socket.getOutputStream().flush();
            messageSequenceNumber++;
        } catch (IOException e) {
            System.err.println("Error sending message: " + e.getMessage());
            cleanup();
        }
    }

    private String createLogonMessage() {
        // First create the message body (everything between tag 35 and checksum)
        StringBuilder body = new StringBuilder()
            .append("35=A|")
            .append("49=").append(SENDER_COMP_ID).append("|")
            .append("56=").append(TARGET_COMP_ID).append("|")
            .append("34=").append(messageSequenceNumber).append("|")
            .append("52=").append(getCurrentTimestamp()).append("|")
            .append("108=").append(HEARTBEAT_INTERVAL);

        // Calculate the body length
        int bodyLength = body.toString().replaceAll("\\|", "\u0001").getBytes(StandardCharsets.UTF_8).length;

        // Create the complete message
        StringBuilder message = new StringBuilder()
            .append("8=FIX.4.2|")
            .append("9=").append(bodyLength).append("|")
            .append(body).append("|");

        // Add the checksum
        String checksumStr = calculateChecksum(message.toString().replaceAll("\\|", "\u0001"));
        message.append("10=").append(checksumStr);

        return message.toString();
    }

    private void sendTestRequest() {
        if (!isRunning) return;

        String testReqId = "TEST" + System.currentTimeMillis();
        
        // Create the message body
        StringBuilder body = new StringBuilder()
            .append("35=1|")
            .append("49=").append(SENDER_COMP_ID).append("|")
            .append("56=").append(TARGET_COMP_ID).append("|")
            .append("34=").append(messageSequenceNumber).append("|")
            .append("52=").append(getCurrentTimestamp()).append("|")
            .append("112=").append(testReqId);

        // Calculate the body length
        int bodyLength = body.toString().replaceAll("\\|", "\u0001").getBytes(StandardCharsets.UTF_8).length;

        // Create the complete message
        StringBuilder message = new StringBuilder()
            .append("8=FIX.4.2|")
            .append("9=").append(bodyLength).append("|")
            .append(body).append("|");

        // Add the checksum
        String checksumStr = calculateChecksum(message.toString().replaceAll("\\|", "\u0001"));
        message.append("10=").append(checksumStr);

        String finalMessage = message.toString();
        System.out.println("Sending test request: " + finalMessage);
        sendMessage(finalMessage);
    }

    private void sendHeartbeat(String testReqId) {
        if (!isRunning) return;

        // Create the message body
        StringBuilder body = new StringBuilder()
            .append("35=0|")
            .append("49=").append(SENDER_COMP_ID).append("|")
            .append("56=").append(TARGET_COMP_ID).append("|")
            .append("34=").append(messageSequenceNumber).append("|")
            .append("52=").append(getCurrentTimestamp());

        if (testReqId != null) {
            body.append("|112=").append(testReqId);
        }

        // Calculate the body length
        int bodyLength = body.toString().replaceAll("\\|", "\u0001").getBytes(StandardCharsets.UTF_8).length;

        // Create the complete message
        StringBuilder message = new StringBuilder()
            .append("8=FIX.4.2|")
            .append("9=").append(bodyLength).append("|")
            .append(body).append("|");

        // Add the checksum
        String checksumStr = calculateChecksum(message.toString().replaceAll("\\|", "\u0001"));
        message.append("10=").append(checksumStr);

        String finalMessage = message.toString();
        System.out.println("Sending heartbeat: " + finalMessage);
        sendMessage(finalMessage);
    }

    private void cleanup() {
        try {
            if (socket != null && !socket.isClosed()) {
                socket.close();
            }
            if (input != null) {
                input.close();
            }
            if (output != null) {
                output.close();
            }
            if (scheduler != null) {
                scheduler.shutdown();
            }
        } catch (IOException e) {
            System.err.println("Error during cleanup: " + e.getMessage());
        }
    }

    private void listenForMessages() throws IOException {
        byte[] buffer = new byte[4096];
        int bytesRead;
        StringBuilder message = new StringBuilder();

        while (isRunning && (bytesRead = socket.getInputStream().read(buffer)) != -1) {
            String chunk = new String(buffer, 0, bytesRead, StandardCharsets.US_ASCII);
            String[] messages = chunk.split("\u0001");

            for (String msg : messages) {
                if (!msg.isEmpty()) {
                    message.append(msg);
                    if (msg.contains("10=")) {
                        String completeMsg = message.toString().replace("\u0001", "|");
                        System.out.println("<<< " + completeMsg);
                        processMessage(completeMsg);
                        message = new StringBuilder();
                    } else {
                        message.append("|");
                    }
                }
            }
        }
    }

    private void processMessage(String message) {
        Map<String, String> fields = parseMessage(message);
        String messageType = fields.get("35");

        if (messageType != null) {
            switch(messageType) {
                case "0":  // Heartbeat
                    //System.out.println("Received heartbeat from " + fields.get("49"));
                    break;
                case "1":  // Test Request
                    //System.out.println("Received test request from " + fields.get("49"));
                    String testReqId = fields.get("112");
                    if (testReqId != null) {
                        sendHeartbeat(testReqId);
                    }
                    break;
                case "2":  // Resend Request
                    //System.out.println("Received resend request");
                    break;
                case "3":  // Reject
                    //System.out.println("Received reject: " + fields.get("58"));
                    break;
                case "5":  // Logout
                    //System.out.println("Received logout");
                    handleLogout();
                    break;
                case "A":  // Logon
                    //System.out.println("Received logon from " + fields.get("49"));
                    // Start sending heartbeats after logon
                    break;
            }
        }
    }

    private Map<String, String> parseMessage(String message) {
        Map<String, String> fields = new HashMap<>();
        String[] pairs = message.split("\\|");
        for (String pair : pairs) {
            String[] keyValue = pair.split("=");
            if (keyValue.length == 2) {
                fields.put(keyValue[0], keyValue[1]);
            }
        }
        return fields;
    }

    private void handleLogout() {
        isRunning = false;
        cleanup();
    }

    private String getCurrentTimestamp() {
        Calendar calendar = Calendar.getInstance();
        int year = calendar.get(Calendar.YEAR);
        int month = calendar.get(Calendar.MONTH) + 1;
        int day = calendar.get(Calendar.DAY_OF_MONTH);
        int hour = calendar.get(Calendar.HOUR_OF_DAY);
        int minute = calendar.get(Calendar.MINUTE);
        int second = calendar.get(Calendar.SECOND);

        return String.format("%04d%02d%02d-%02d:%02d:%02d", 
            year, month, day, hour, minute, second);
    }

    private String calculateChecksum(String message) {
        int sum = 0;
        for (byte b : message.getBytes(StandardCharsets.US_ASCII)) {
            sum += b & 0xFF;
        }
        return String.format("%03d", sum % 256);
    }
}
