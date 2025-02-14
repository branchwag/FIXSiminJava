import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class FIXClient {

	private static final String SERVER_HOST = "127.0.0.1";
	private static final int SERVER_PORT = 5000;
	private static final String SENDER_COMP_ID = "TESTINJAVA";
	private static final String TARGET_COMP_ID = "MINIFIX";

	private Socket socket;
	private BufferedReader input;
	private PrintWriter output;

	public static void main(String[] args) {
		FIXClient client = new FIXClient();
		try {
			client.connect();
			client.sendLogonMessage();
			client.listenForMessages();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void connect() throws IOException {
		socket = new Socket(SERVER_HOST, SERVER_PORT);
		input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
		output = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()), true);
		System.out.println("Connected to FIX server at " + SERVER_HOST + ":" + SERVER_PORT);
	}

	private void sendLogonMessage() {
		String logonMessage = createLogonMessage();
		System.out.println("Sending logon message: " + logonMessage);
		output.println(logonMessage);
	}

	private String createLogonMessage() {
		StringBuilder message = new StringBuilder();
		message.append("8=FIX.4.2|");
		message.append("9=").append(generateMessageLength("8=FIX.4.2|9=|35=A|49=" + SENDER_COMP_ID + "|56=" + TARGET_COMP_ID)).append("|");
		message.append("35=A|");  // Logon message type
		message.append("49=").append(SENDER_COMP_ID).append("|");  // SenderCompID
		message.append("56=").append(TARGET_COMP_ID).append("|");  // TargetCompID
		message.append("34=1|");  // Message sequence number
		message.append("52=").append(getCurrentTimestamp()).append("|");  // Sending time
		message.append("108=30|");  // Heartbeat interval (in seconds)
		message.append("10=").append(calculateChecksum(message.toString())).append("|");  // Checksum
			
		return message.toString().replaceAll("\\|$", "");
	}

	private void listenForMessages() throws IOException {
		String incomingMessage;
		while ((incomingMessage = input.readLine()) != null) {
			System.out.println("Received message: " + incomingMessage);
		}
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
