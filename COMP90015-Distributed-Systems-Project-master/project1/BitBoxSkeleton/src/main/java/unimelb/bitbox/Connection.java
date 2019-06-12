package unimelb.bitbox;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.logging.Logger;

import org.json.simple.JSONObject;

import unimelb.bitbox.util.Configuration;
import unimelb.bitbox.util.Document;
import unimelb.bitbox.util.HostPort;
import unimelb.bitbox.util.FileSystemManager.FileSystemEvent;

/**
 * Deal with things about socket including sending and receiving message.
 */
public class Connection extends Thread {
	private static Logger log = Logger.getLogger(Peer.class.getName());
	private ServerMain server = null;
	private Socket connectedSocket;
	private long blockSize;
	private BufferedReader reader;
	private BufferedWriter writer;
	private String host;
	private int port;
	private String connectedHost;
	private int connectedPort;

	/**
	 * when server connects other server, use this constructor to create an
	 * object of Class Connection to monitor.
	 */
	public Connection(ServerMain server, Socket socket) throws IOException {
		setCommonAttributesValue(server, socket);
		start();
	}

	/**
	 * when server receives a connection, use this constructor to create
	 * an object of Class Connection to monitor.
	 */
	public Connection(ServerMain server, Socket socket, String connectedHost, int connectedPort)
			throws IOException {
		setCommonAttributesValue(server, socket);
		this.connectedHost = connectedHost;
		this.connectedPort = connectedPort;
		start();
	}

	/**
	 * Set common attributes value for constructor.
	 */
	private void setCommonAttributesValue(ServerMain server, Socket socket) throws IOException {
		this.server = server;
		host = Configuration.getConfigurationValue("advertisedName");
		port = Integer.parseInt(Configuration.getConfigurationValue("port"));
		blockSize = Long.parseLong(Configuration.getConfigurationValue("blockSize"));
		connectedSocket = socket;
		reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), "UTF-8"));
		writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), "UTF-8"));
	}

	public void run() {
		String data;
		try {
			while ((data = reader.readLine()) != null) {
				// System.out.println(data);
				// convert message from string to JSON
				Document doc = Document.parse(data);
				checkCommand(doc);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			// e.printStackTrace();
			try {
				connectedSocket.close();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
	}

	/**
	 * broadcast message to the clients.
	 * 
	 * @param doc the message you want to broadcast.
	 */
	public void sendMessage(Document doc) {
		try {
			writer.write(doc.toJson() + "\n");
			writer.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			log.info("Fail to send message to connected peer.");
		}

	}

	/**
	 * check command information and response.
	 * 
	 * @param doc received message.
	 * 
	 */
	public void checkCommand(Document doc) throws IOException {
		String command = doc.getString("command");

		/* receive HANDSHAKE_REQUEST */
		if (command.equals("HANDSHAKE_REQUEST")) {
			Document hostPort = (Document) doc.get("hostPort");
			// System.out.println(hostPort.toJson());
			connectedHost = hostPort.getString("host");
			String temp = "" + hostPort.get("port");
			connectedPort = Integer.parseInt(temp);
			// log.info("received " + command + " from " + connectedHost + ":" +
			// connectedPort);
			log.info("current incoming connection is " + ServerMain.currentIncomingconnectionNum + " max incoming connection is " + ServerMain.maximunIncommingConnections);
			if (server.connectedPeerListContains(connectedHost + ":" + connectedPort)) {
				invalidProtocol();
			} else if (ServerMain.currentIncomingconnectionNum >= ServerMain.maximunIncommingConnections) {
				connectionRefused();
			} else {
				handshakeResponse();
			}
		}

		log.info("received " + command + " from " + connectedHost + ":" + connectedPort);

		/* receive HANDSHAKE_RESPONSE */
		if (command.equals("HANDSHAKE_RESPONSE")) {
			Document hostPort = (Document) doc.get("hostPort");
			// System.out.println(hostPort.toJson());
			connectedHost = hostPort.getString("host");
			String temp = "" + hostPort.get("port");
			connectedPort = Integer.parseInt(temp);
			// mark as successful connection
			if(server.connectedPeerListPut(connectedHost + ":" + connectedPort, this) == false) {
				connectedSocket.close();
			}
			/* sync at the beginning of a connection
			for(FileSystemEvent pathevent : ServerMain.fileSystemManager.generateSyncEvents()) {
				log.info(pathevent.toString());
				server.processFileSystemEvent(pathevent);
			}*/
			// log.info("received " + command + " from " + connectedHost + ":" +
			// connectedPort);
		}

		/* receive CONNECTION_REFUSED */
		if (command.equals("CONNECTION_REFUSED")) {
			// log.info("received " + command + " from " + connectedHost + ":" +
			// connectedPort);
			connectedSocket.close();
		}

		/* receive INVALID_PROTOCOL */
		if (command.equals("INVALID_PROTOCOL")) {
			// log.info("received " + command + " from " + connectedHost + ":" +
			// connectedPort);
			connectedSocket.close();
		}

		/* receive FILE_CREATE_REQUEST */
		if (command.equals("FILE_CREATE_REQUEST")) {
			fileCreateResponse(doc);
		}

		/* receive FILE_BYTES_REQUEST */
		if (command.equals("FILE_BYTES_REQUEST")) {
			try {
				fileBytesResponse(doc);
			} catch (NoSuchAlgorithmException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		/* receive FILE_BYTES_RESPONSE */
		if (command.equals("FILE_BYTES_RESPONSE")) {
			String pathName = doc.getString("pathName");
			long position = doc.getLong("position");
			String content = doc.getString("content");
			ByteBuffer byteBuffer = ByteBuffer.wrap(Base64.getDecoder().decode(content));
			ServerMain.fileSystemManager.writeFile(pathName, byteBuffer, position);
			log.info("Test whether is finished!");
			try {
				if (!ServerMain.fileSystemManager.checkWriteComplete(pathName)) {
					log.info("writing is not finished!");
					fileBytesRequest(doc);
				} else {
					log.info("writing is finished!");
				}
			} catch (NoSuchAlgorithmException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		/* receive FILE_DELETE_REQUEST */
		if (command.equals("FILE_DELETE_REQUEST")) {
			fileDeleteResponse(doc);
		}

		/*
		 * receive FILE_DELETE_RESPONSE if(command.equals("FILE_DELETE_RESPONSE")) {
		 * already log.info log.info(doc.getString("message")); }
		 */

		/* receive FILE_MODIFY_REQUEST */
		if (command.equals("FILE_MODIFY_REQUEST")) {
			try {
				fileModifyResponse(doc);
			} catch (NoSuchAlgorithmException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		/* receive DIRECTORY_CREATE_REQUEST */
		if (command.equals("DIRECTORY_CREATE_REQUEST")) {
			directoryCreateResponse(doc);
		}

		/* receive DIRECTORY_DELETE_REQUEST */
		if (command.equals("DIRECTORY_DELETE_REQUEST")) {
			directoryDeleteResponse(doc);
		}
		// log.info("received " + command + " from " + connectedHost + ":" +
		// connectedPort);
	}

	/**
	 * @author yuqiangz@student.unimelb.edu.au
	 */
	public void handshakeResponse() throws IOException {
		Document doc = new Document();
		doc.append("command", "HANDSHAKE_RESPONSE");
		doc.append("hostPort", new HostPort(host, port).toDoc());
		sendMessage(doc);
		log.info("sending to " + connectedHost + ":" + connectedPort + doc.toJson());
		// update the num of incoming connection
		ServerMain.currentIncomingconnectionNum++;
		// mark as successful connection
		if(server.connectedPeerListPut(connectedHost + ":" + connectedPort, this) == false) {
			connectedSocket.close();
		}
		/* sync at the beginning of a connection
		for(FileSystemEvent pathevent : ServerMain.fileSystemManager.generateSyncEvents()) {
			log.info(pathevent.toString());
			server.processFileSystemEvent(pathevent);
		}*/
	}

	/**
	 * @author yuqiangz@student.unimelb.edu.au
	 */
	public void handshakeRequest() {
		Document doc = new Document();
		doc.append("command", "HANDSHAKE_REQUEST");
		doc.append("hostPort", new HostPort(host, port).toDoc());
		sendMessage(doc);
		log.info("sending to " + connectedHost + ":" + connectedPort + doc.toJson());
	}

	/**
	 * @author yuqiangz@student.unimelb.edu.au
	 */
	public void connectionRefused() throws IOException {
		Document doc = new Document();
		doc.append("command", "CONNECTION_REFUSED");
		doc.append("message", "connection limit reached");
		ArrayList<Document> peerDocList = new ArrayList<Document>();
		HashMap<String, Connection> connectedPeerList = server.getConnectedPeerList();
		for (String peer : connectedPeerList.keySet()) {
			Document peerDoc = new Document();
			String host = (peer.split(":"))[0];
			int port = Integer.parseInt((peer.split(":"))[1]);
			peerDoc.append("host", host);
			peerDoc.append("port", port);
			peerDocList.add(peerDoc);
		}
		doc.append("peers", peerDocList);
		sendMessage(doc);
		log.info("sending to " + connectedHost + ":" + connectedPort + doc.toJson());
		connectedSocket.close();
	}

	/**
	 * @author yuqiangz@student.unimelb.edu.au
	 */
	public void invalidProtocol() {
		Document doc = new Document();
		doc.append("command", "INVALID_PROTOCOL");
		doc.append("message", "message must a command field as string");
		sendMessage(doc);
		log.info("sending to " + connectedHost + ":" + connectedPort + doc.toJson());
		try {
			connectedSocket.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		server.connectedPeerListRemove(connectedHost + ":" + connectedPort);
	}

	/**
	 * @author yuqiangz@student.unimelb.edu.au
	 * 
	 * @param message the content of FILE_CREATE_REQUEST
	 */
	public void fileCreateResponse(Document message) {
		String pathName = message.getString("pathName");
		System.out.println(pathName);
		Document fileDescriptor = (Document) message.get("fileDescriptor");
		String md5 = fileDescriptor.getString("md5");
		long length = fileDescriptor.getLong("fileSize");
		long lastModified = fileDescriptor.getLong("lastModified");

		Document doc = new Document();
		doc.append("command", "FILE_CREATE_RESPONSE");
		doc.append("fileDescriptor", fileDescriptor);
		doc.append("pathName", pathName);
		log.info("pathName is " + pathName);
		if (!ServerMain.fileSystemManager.isSafePathName(pathName)) {
			doc.append("message", "unsafe pathname given");
			doc.append("status", false);
			sendMessage(doc);
			log.info("sending to " + connectedHost + ":" + connectedPort + doc.toJson());
			return;
		}
		log.info(pathName + " is safe path name");
		if (ServerMain.fileSystemManager.fileNameExists(pathName)) {
			doc.append("message", "pathname already exists");
			doc.append("status", false);
			sendMessage(doc);
			log.info("sending to " + connectedHost + ":" + connectedPort + doc.toJson());
			return;
		}
		log.info(pathName + " doesn't exist bebore.");
		try {
			if (ServerMain.fileSystemManager.createFileLoader(pathName, md5, length, lastModified)) {
				log.info("create file loader successfully!");
				try {
					if (ServerMain.fileSystemManager.checkShortcut(pathName)) {
						doc.append("message", "use a local copy");
						doc.append("status", true);
						sendMessage(doc);
					} else {
						doc.append("message", "file loader ready");
						doc.append("status", true);
						sendMessage(doc);
						fileBytesRequest(message);
					}
				} catch (NoSuchAlgorithmException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				log.info("sending to " + connectedHost + ":" + connectedPort + doc.toJson());
				return;
			} else {
				log.info("fail to create file loader");
				doc.append("message", "there was a problem creating the file");
				doc.append("status", false);
				sendMessage(doc);
				log.info("sending to " + connectedHost + ":" + connectedPort + doc.toJson());
				return;
			}
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * @author yuqiangz@student.unimelb.edu.au
	 * 
	 * @param message the content of FILE_CREATE_REQUEST or FILE_BYTES_RESPONSE
	 *                which doesn't complete write.
	 */
	public void fileBytesRequest(Document message) {
		Document fileDescriptor = (Document) message.get("fileDescriptor");
		Document doc = new Document();
		String receivedCommand = message.getString("command");
//			if(receivedCommand.equals("FILE_MODIFY_REQUEST")) {
//			    receivedCommand = "FILE_BYTES_REQUEST";
//			}

		doc.append("command", "FILE_BYTES_REQUEST");
		doc.append("fileDescriptor", fileDescriptor);
		doc.append("pathName", message.getString("pathName"));
		long fileSize = fileDescriptor.getLong("fileSize");
		if (receivedCommand.equals("FILE_CREATE_REQUEST") || receivedCommand.equals("FILE_MODIFY_REQUEST")) {
			doc.append("position", 0);

			if (fileSize > blockSize) {
				doc.append("length", blockSize);
			} else {
				doc.append("length", fileSize);
			}
		}
		if (receivedCommand.equals("FILE_BYTES_RESPONSE")) {
			long startPos = message.getLong("position") + message.getLong("length");
			doc.append("position", startPos);
			if (startPos + blockSize > fileSize) {
				doc.append("length", fileSize - startPos);
			} else {
				doc.append("length", blockSize);
			}
		}
		sendMessage(doc);
		log.info("sending to " + connectedHost + ":" + connectedPort + doc.toJson());
	}

	/**
	 * @author yuqiangz@student.unimelb.edu.au
	 * 
	 * @param message the content of FILE_BYTES_REQUEST
	 */
	public void fileBytesResponse(Document message) throws NoSuchAlgorithmException, IOException {
		Document doc = new Document();
		Document fileDescriptor = (Document) message.get("fileDescriptor");

		doc.append("command", "FILE_BYTES_RESPONSE");
		doc.append("fileDescriptor", fileDescriptor);
		doc.append("pathName", message.getString("pathName"));
		doc.append("position", message.getLong("position"));
		doc.append("length", message.getLong("length"));

		long startPos = message.getLong("position");
		long length = message.getLong("length");
		String md5 = fileDescriptor.getString("md5");
		ByteBuffer byteBuffer = ServerMain.fileSystemManager.readFile(md5, startPos, length);
		String encodedString = Base64.getEncoder().encodeToString(byteBuffer.array());
		doc.append("content", encodedString);
		if (byteBuffer.array() == null) {
			doc.append("message", "unsuccessful read");
			doc.append("status", false);
		}

		doc.append("message", "successful read");
		doc.append("status", true);
		sendMessage(doc);
		log.info("sending to " + connectedHost + ":" + connectedPort + doc.toJson());
	}

	/**
	 * @author laif1
	 * 
	 * @param message the content of FILE_DELETE_REQUEST
	 */
	public void fileDeleteResponse(Document message) {
		String pathName = message.getString("pathName");
		System.out.println(pathName);
		Document fileDescriptor = (Document) message.get("fileDescriptor");
		String md5 = fileDescriptor.getString("md5");
		long lastModified = fileDescriptor.getLong("lastModified");

		Document doc = new Document();
		doc.append("command", "FILE_DELETE_RESPONSE");
		doc.append("fileDescriptor", fileDescriptor);
		doc.append("pathName", message.getString("pathName"));

		if (!ServerMain.fileSystemManager.isSafePathName(pathName)) {
			doc.append("message", "unsafe pathname given");
			doc.append("status", false);
			sendMessage(doc);
			log.info("sending to " + connectedHost + ":" + connectedPort + doc.toJson());
			return;
		}

		if (!ServerMain.fileSystemManager.fileNameExists(pathName)) {
			doc.append("message", "pathname does not exist");
			doc.append("status", false);
			sendMessage(doc);
			log.info("sending to " + connectedHost + ":" + connectedPort + doc.toJson());
			return;
		}

		boolean deleteStatus = ServerMain.fileSystemManager.deleteFile(pathName, lastModified, md5);
		if (deleteStatus) {
			log.info("file deleted");
			doc.append("message", "File Deleted");
			doc.append("status", true);
		} else {
			log.info("pathname does not exist");
			doc.append("message", "there was a problem deleting the file");
			doc.append("status", false);
		}
		sendMessage(doc);
		log.info("sending to " + connectedHost + ":" + connectedPort + doc.toJson());
	}
	
	/**
	 * @author laif1
	 * 
	 * @param message the content of FILE_DELETE_REQUEST
	 */
	public void fileModifyResponse(Document message) throws IOException, NoSuchAlgorithmException {
		String pathName = message.getString("pathName");
		System.out.println(pathName);
		Document fileDescriptor = (Document) message.get("fileDescriptor");
		String md5 = fileDescriptor.getString("md5");
		long lastModified = fileDescriptor.getLong("lastModified");

		boolean modifyLoaderStatus = ServerMain.fileSystemManager.modifyFileLoader(pathName, md5, lastModified);

		Document doc = new Document();
		doc.append("command", "fileModifyResponse");
		doc.append("fileDescriptor", fileDescriptor);
		doc.append("pathName", pathName);
		log.info("pathName is " + pathName);

		if (!ServerMain.fileSystemManager.isSafePathName(pathName)) {
			doc.append("message", "unsafe pathname given");
			doc.append("status", false);
			sendMessage(doc);
			log.info("sending to " + connectedHost + ":" + connectedPort + doc.toJson());
			return;
		}
		log.info(pathName + " is safe pathname");

		if (!ServerMain.fileSystemManager.fileNameExists(pathName)) {
			doc.append("message", "pathname does not exist");
			doc.append("status", false);
			sendMessage(doc);
			log.info("sending to " + connectedHost + ":" + connectedPort + doc.toJson());
			return;
		}

		if (modifyLoaderStatus) {
			log.info("create file loader successfully!");
			try {
				if (ServerMain.fileSystemManager.checkShortcut(pathName)) {
					doc.append("message", "file already exists with matching content");
					doc.append("status", true);
					sendMessage(doc);
				} else {
					System.out.print("It works");
					doc.append("message", "file loader ready");
					doc.append("status", true);
					sendMessage(doc);
					System.out.print("It works modify");
					fileBytesRequest(message); // need to test
				}
			} catch (NoSuchAlgorithmException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			log.info("sending to " + connectedHost + ":" + connectedPort + doc.toJson());
			return;
		} else {
			log.info("fail to create file loader");
			doc.append("message", "there was a problem modifying the file");
			doc.append("status", false);
			sendMessage(doc);
			log.info("sending to " + connectedHost + ":" + connectedPort + doc.toJson());
			return;
		}
	}

	/**
	 * @author laif1
	 */
	public void directoryCreateResponse(Document message) {
		String pathName = message.getString("pathName");

		Document doc = new Document();
		doc.append("command", "DIRECTORY_CREATE_RESPONSE");
		doc.append("pathName", message.getString("pathName"));
		
		if (!ServerMain.fileSystemManager.isSafePathName(pathName)) {
			doc.append("message", "unsafe pathname given");
			doc.append("status", false);
			sendMessage(doc);
			log.info("sending to " + connectedHost + ":" + connectedPort + doc.toJson());
			return;
		}
		log.info(pathName + " is safe pathname");

		if (ServerMain.fileSystemManager.fileNameExists(pathName)) {
			doc.append("message", "pathname already exists");
			doc.append("status", false);
			sendMessage(doc);
			log.info("sending to " + connectedHost + ":" + connectedPort + doc.toJson());
			return;
		}
		
		boolean directoryCreateStatus = ServerMain.fileSystemManager.makeDirectory(pathName);
		if (directoryCreateStatus) {
			log.info("file created");
			doc.append("message", "directory created");
		} else {
			log.info("pathname does not exist");
			doc.append("message", "there was a problem creating the directory");
		}
		doc.append("status", directoryCreateStatus);
		sendMessage(doc);
		log.info("sending to " + connectedHost + ":" + connectedPort + doc.toJson());
	}

	/**
	 * @author laif1
	 * 
	 * @param message the content of DIRECTORY_DELETE_REQUEST
	 */
	public void directoryDeleteResponse(Document message) {
		String pathName = message.getString("pathName");
		
		Document doc = new Document();
		doc.append("command", "DIRECTORY_DELETE_RESPONSE");
		doc.append("pathName", message.getString("pathName"));
		
		if (!ServerMain.fileSystemManager.isSafePathName(pathName)) {
			doc.append("message", "unsafe pathname given");
			doc.append("status", false);
			sendMessage(doc);
			log.info("sending to " + connectedHost + ":" + connectedPort + doc.toJson());
			return;
		}

		if (!ServerMain.fileSystemManager.dirNameExists(pathName)) {
			doc.append("message", "direName does not exist");
			doc.append("status", false);
			sendMessage(doc);
			log.info("sending to " + connectedHost + ":" + connectedPort + doc.toJson());
			return;
		}
		
		boolean directoryDeleteStatus = ServerMain.fileSystemManager.deleteDirectory(pathName);
		if (directoryDeleteStatus) {
			log.info("file deleted");
			doc.append("message", "directory delete");
		} else {
			log.info("pathname does not exist");
			doc.append("message", "there was a problem creating the directory");
		}
		doc.append("status", directoryDeleteStatus);
		sendMessage(doc);
		log.info("sending to " + connectedHost + ":" + connectedPort + doc.toJson());
	}
}
