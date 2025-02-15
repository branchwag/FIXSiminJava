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

		scheduler = Executors.newScheduledThreadPool(1);
		scheduler.scheduleAtFixedRate(this::sendTestRequest, HEARTBEAT_INTERVAL/2, HEARTBEAT_INTERVAL/2, TimeUnit.SECONDS);

		sendLogonMessage();
		listenForMessages();
	}

	private void connect() throws IOException {
		socket = new Socket(SERVER_HOST, SERVER_PORT);
		socket.setKeepAlive(true);
		socket.setSoTimeout(HEARTBEAT_INTERVAL * 2 * 1000);
		input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
		output = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()), true);
		System.out.println("Connected to FIX server at " + SERVER_HOST + ":" + SERVER_PORT);
	}

	private void sendLogonMessage() {
		String logonMessage = createLogonMessage();
		System.out.println("Sending logon message: " + logonMessage);
		//output.println(logonMessage);
		sendMessage(logonMessage);
	}

	private synchronized void sendMessage(String message) {
		output.println(message);
		messageSequenceNumber++;
	}

	private String createLogonMessage() {
		StringBuilder message = new StringBuilder();
		String header = "8=FIX.4.2|9=|35=A|49=" + SENDER_COMP_ID + "|56=" + TARGET_COMP_ID;

		message.append("8=FIX.4.2|");
		message.append("9=").append(generateMessageLength(header)).append("|");
		message.append("35=A|");
		message.append("49=").append(SENDER_COMP_ID).append("|");
		message.append("56=").append(TARGET_COMP_ID).append("|");
		message.append("34=").append(messageSequenceNumber).append("|");
		message.append("52=").append(getCurrentTimestamp()).append("|");
		message.append("108=").append(HEARTBEAT_INTERVAL).append("|");
		message.append("10=").append(calculateChecksum(message.toString())).append("|");
			
		return message.toString().replaceAll("\\|$", "");
	}

	private void sendTestRequest() {
		if (!isRunning) return;

		StringBuilder message = new StringBuilder();
		String testReqId = "TEST" + System.currentTimeMillis();
		String header = "8=FIX.4.2|9=|35=1|49=" + SENDER_COMP_ID + "|56=" + TARGET_COMP_ID;
		
		message.append("8=FIX.4.2|");
		message.append("9=").append(generateMessageLength(header)).append("|");
		message.append("35=1|");  // Test Request message type
		message.append("49=").append(SENDER_COMP_ID).append("|");
		message.append("56=").append(TARGET_COMP_ID).append("|");
		message.append("34=").append(messageSequenceNumber).append("|");
		message.append("52=").append(getCurrentTimestamp()).append("|");
		message.append("112=").append(testReqId).append("|");  // TestReqID
		message.append("10=").append(calculateChecksum(message.toString())).append("|");
		
		System.out.println("Sending test request: " + message.toString().replaceAll("\\|$", ""));
		sendMessage(message.toString().replaceAll("\\|$", ""));
	}

	private void sendHeartbeat(String testReqId) {
		if (!isRunning) return;

		StringBuilder message = new StringBuilder();
		String header = "8=FIX.4.2|9=|35=0|49=" + SENDER_COMP_ID + "|56=" + TARGET_COMP_ID;

		message.append("8=FIX.4.2|");
		message.append("9=").append(generateMessageLength(header)).append("|");
		message.append("35=0|");
		message.append("49=").append(SENDER_COMP_ID).append("|");
		message.append("56=").append(TARGET_COMP_ID).append("|");
		message.append("34=").append(messageSequenceNumber).append("|");
		message.append("52=").append(getCurrentTimestamp()).append("|");
		if (testReqId != null) {
			message.append("112=").append(testReqId).append("|");
		}
		message.append("10=").append(calculateChecksum(message.toString())).append("|");

		System.out.println("Sending heartbeat: " + message.toString().replaceAll("\\|$", ""));
		sendMessage(message.toString().replaceAll("\\|$", ""));
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
		String incomingMessage;
		while (isRunning && (incomingMessage = input.readLine()) != null) {
			//System.out.println("Received message: " + incomingMessage);
			processMessage(incomingMessage);
		}
	}

	private void processMessage(String message) {
		System.out.println("Received message: " + message);

		Map<String, String> fields = parseMessage(message);
		String messageType = fields.get("35");


		if (messageType != null) {
			switch(messageType) {
				case "0":
					break;
				case "1":
					String testReqId = fields.get("112");
					if (testReqId != null) {
						sendHeartbeat(testReqId);
					}
					break;
				case "5": 
					handleLogout();
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

	private String generateMessageLength(String message) {
		return String.valueOf(message.getBytes(StandardCharsets.UTF_8).length);
	}

	private String getCurrentTimestamp() {
		Calendar calendar = Calendar.getInstance();
		int year = calendar.get(Calendar.YEAR);
		int month = calendar.get(Calendar.MONTH) + 1;
		int day = calendar.get(Calendar.DAY_OF_MONTH);
		int hour = calendar.get(Calendar.HOUR_OF_DAY);
		int minute = calendar.get(Calendar.MINUTE);
		int second = calendar.get(Calendar.SECOND);

		return String.format("%04d%02d%02d-%02d:%02d:%02d", year, month, day, hour, minute, second);
	}

	private String calculateChecksum(String message) {
		int checksum = 0;
		for (int i = 0; i < message.length(); i++) {
			checksum += message.charAt(i);
		}
		return String.format("%03d", checksum % 256);
	}
}
