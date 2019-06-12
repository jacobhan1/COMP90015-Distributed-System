package unimelb.bitbox;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Logger;

import unimelb.bitbox.util.Configuration;
import unimelb.bitbox.util.Document;
import unimelb.bitbox.util.FileSystemManager;
import unimelb.bitbox.util.FileSystemObserver;
import unimelb.bitbox.util.FileSystemManager.EVENT;
import unimelb.bitbox.util.FileSystemManager.FileSystemEvent;

public class ServerMain extends Thread implements FileSystemObserver {
	private static Logger log = Logger.getLogger(ServerMain.class.getName());
	protected static FileSystemManager fileSystemManager;
	private ServerSocket serverSocket;	
	/**
	 * Assume every peer's name(host:port) is different, key is Peer's name,
	 * collect objects of class Connection after passing the handshake process.
	 */
	private volatile HashMap<String, Connection> connectedPeerList;
	protected volatile static int currentIncomingconnectionNum = 0;
	protected static int maximunIncommingConnections = 
			Integer.parseInt(Configuration.getConfigurationValue("maximumIncommingConnections"));
	
	public ServerMain() throws NumberFormatException, IOException, NoSuchAlgorithmException {
		fileSystemManager=new FileSystemManager(Configuration.getConfigurationValue("path"),this);
		connectedPeerList = new HashMap<String, Connection>();
		
		// set server to receive incoming connections
		int port = Integer.parseInt(Configuration.getConfigurationValue("port"));
		try {
			serverSocket = new ServerSocket(port);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// ready to receive incoming connections
		start();
		
		// try to connect peers
		connectPeer();	
		
		// Every specified seconds, sync with all connected peers
		//syncWithPeers();
	}
	
	
	private void connectPeer() {
		synchronized (connectedPeerList) {
			for (String peer : Configuration.getConfigurationValue("peers").split(",")) {
				// already connected
				if (connectedPeerList.containsKey(peer)) {
					continue;
				}
				
				String destHost = (peer.split(":"))[0];
				int destPort = Integer.parseInt((peer.split(":"))[1]);
				try {
					Socket clientSocket = new Socket(destHost, destPort);
					log.info("connect to " + peer + " successfully.");
					Connection connection = new Connection(this, clientSocket, destHost, destPort);
					// send HANDSHAKE_REQUEST
					connection.handshakeRequest();
					//socketList.add(clientSocket);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					log.warning("while connecting to " + peer + " refused.");
				}
			}
		}
	}

	/**
	 * 
	 * @return a copy of connectedPeerList
	 */
	public HashMap<String, Connection> getConnectedPeerList() {
		return new HashMap<String, Connection>(connectedPeerList);
	}

	public synchronized Boolean connectedPeerListPut(String peer, Connection connection) {
		if(connectedPeerList.containsKey(peer)) {
			return false;
		}
		connectedPeerList.put(peer, connection);
		return true;
	}
	
	public synchronized Boolean connectedPeerListContains(String peer) {
		return connectedPeerList.containsKey(peer);
	}
	
	public void connectedPeerListRemove(String peer) {
		connectedPeerList.remove(peer);
	}
	
	/**
	 * Every specified seconds, sync with all connected peers
	 * 
	 * @author yuqiangz@student.unimelb.edu.au
	 */
	public void syncWithPeers() {
		Timer timer = new Timer();
		long syncPeriod = Long.parseLong(Configuration.getConfigurationValue("syncInterval")) * 1000;
		timer.schedule(new TimerTask() {
			public void run() {
				log.info("sync with all connected peers");
				for(FileSystemEvent pathevent : fileSystemManager.generateSyncEvents()) {
					log.info(pathevent.toString());
					processFileSystemEvent(pathevent);
				}
			}
		}, syncPeriod, syncPeriod);
	}
	
	public void run() {
		while(true) {
			Socket clientSocket;
			try {
				// wait for receive connection
				clientSocket = serverSocket.accept();
				new Connection(this, clientSocket);
				//log.info("get connect request from " + clientSocket.getInetAddress().getHostName() 
					//	+ clientSocket.getPort());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	@Override
	public void processFileSystemEvent(FileSystemEvent fileSystemEvent) {
		// TODO: process events
		switch (fileSystemEvent.event) {
			case FILE_CREATE: {
				fileCreateRequest(fileSystemEvent);
				break;
			}
			case FILE_DELETE: {
			    fileDeleteRequest(fileSystemEvent);
				break;
			}
			case FILE_MODIFY: {
			    fileModifyRequest(fileSystemEvent);
				break;
			}
			case DIRECTORY_CREATE: {
			    directoryCreateRequest(fileSystemEvent);
				break;
			}
			case DIRECTORY_DELETE: {
			    directoryDeleteRequest(fileSystemEvent);
				break;
			}
		}
	}
	
	/**
	 * Broadcast a message to every connected peer.
	 * 
	 * @param doc the message
	 */
	private void broadcastToPeers(Document doc) {
		for(String peer: connectedPeerList.keySet()) {
			connectedPeerList.get(peer).sendMessage(doc);
			log.info("sending to " + peer + doc.toJson());
		}
	}
	
	/**
	 * @author yuqiangz@student.unimelb.edu.au
	 */
	public void fileCreateRequest(FileSystemEvent fileSystemEvent) {
		Document doc = new Document();
		doc.append("command", "FILE_CREATE_REQUEST");
		doc.append("fileDescriptor", fileSystemEvent.fileDescriptor.toDoc()); 
		doc.append("pathName", fileSystemEvent.pathName);
		// delete it after debug
		if(fileSystemEvent.pathName.endsWith("(bitbox)")) {
			log.info("It's suffix file.");
			return;
		}
		broadcastToPeers(doc);
	}
	
	/**
     * @author laif1
     */
	public void fileDeleteRequest(FileSystemEvent fileSystemEvent) {
	    //System.out.print("deleted request used");
	    Document doc = new Document();
	    doc.append("command", "FILE_DELETE_REQUEST");
	    doc.append("fileDescriptor", fileSystemEvent.fileDescriptor.toDoc());
	    doc.append("pathName", fileSystemEvent.pathName);
        broadcastToPeers(doc);
	}
	
	/**
     * @author laif1
     */
	public void fileModifyRequest(FileSystemEvent fileSystemEvent) {
	    Document doc = new Document();
        doc.append("command", "FILE_MODIFY_REQUEST");
        doc.append("fileDescriptor", fileSystemEvent.fileDescriptor.toDoc());
        doc.append("pathName", fileSystemEvent.pathName);
        broadcastToPeers(doc);
	}
	
	/**
     * @author laif1
     */
	public void directoryCreateRequest(FileSystemEvent fileSystemEvent) {
	    Document doc = new Document();
        doc.append("command", "DIRECTORY_CREATE_REQUEST");
        doc.append("pathName", fileSystemEvent.pathName);
        broadcastToPeers(doc);
	}
	
	/**
     * @author laif1
     */
	public void directoryDeleteRequest(FileSystemEvent fileSystemEvent) {
        Document doc = new Document();
        doc.append("command", "DIRECTORY_DELETE_REQUEST");
        doc.append("pathName", fileSystemEvent.pathName);
        broadcastToPeers(doc);
    }
	
}
