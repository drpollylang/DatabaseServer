package edu.uob;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Paths;
import java.nio.file.Files;

// TODO: refactor - contains attribute that is a map of Tables
// TODO: Passed to DBCommand objects - access and write to the tables

// TODOs:
// Create and instantiate a Condition class - use to write methods in DBTable for getting the index of a row/column
//    by condition.
// Implement and test the manipulations (read/write also)
// DBServer contains a hashmap of Tables (keys = tablenames, values = Tables) - read in initially. Write whenever updated.
// Refactor DBTable with table data as a list of lists
// Refactor command types into different classes that are extensions of an abstract DBCommand
// Refactor handler into methods within the command classes (which are passed the DBServer to access data)

/** This class implements the DB server. */
public class DBServer {

    private static final char END_OF_TRANSMISSION = 4;
    private String storageFolderPath;


    public static void main(String args[]) throws IOException {
        DBServer server = new DBServer();
        server.blockingListenOn(8888);
    }

    /**
    * KEEP this signature otherwise we won't be able to mark your submission correctly.
    */
    public DBServer() {
        storageFolderPath = Paths.get("databases").toAbsolutePath().toString();
        try {
            // Create the database storage folder if it doesn't already exist !
            Files.createDirectories(Paths.get(storageFolderPath));
        } catch(IOException ioe) {
            System.out.println("Can't seem to create database storage folder " + storageFolderPath);
        }
    }

    /**
    * KEEP this signature (i.e. {@code edu.uob.DBServer.handleCommand(String)}) otherwise we won't be
    * able to mark your submission correctly.
    *
    * <p>This method handles all incoming DB commands and carries out the required actions.
    */
    public String handleCommand(String command) {
        // TODO implement your server logic here

        // Instantiate a new QueryHandler to handle the command
        // Pass command to QueryHandler, receive results from QueryHandler, pass them back
        // // to DBClient for displaying to console
        try {
            QueryHandler queryHandler = new QueryHandler(command);
            String responseCode = queryHandler.getResponseCode(); // TODO: implement this in QueryHandler
            String responseMessage = queryHandler.getResponseMessage(); // TODO: implement this in QueryHandler
            return responseCode + "\r\n" + responseMessage;
        } catch (DBException exception) {
            return exception.getMessage();
        }

        // Receive results
        /*
        String databaseName = "".toLowerCase();
        String tableName = "".toLowerCase();
        String tableFilePath = getFilePath(databaseName, tableName);

        try {
            readData(tableFilePath);
        } catch (IOException readDataException) {
            System.out.println("Error reading in data: malformed line in table " + tableName);
        }
        */
    }


    // Convert database name and table name to a filepath that can be used
    // to instantiate a DBTable
    private String getFilePath(String databaseName, String tableName) {
        return this.storageFolderPath + File.separator + databaseName + File.separator + tableName;
    }

    private boolean checkFileExists(String filePath) {
        File fileToBeChecked = new File(filePath);
        return fileToBeChecked.exists();
    }

    // Create a database
    public String createDatabase(String databaseName) {
        databaseName = databaseName.toLowerCase();
        String databaseFilePath = storageFolderPath + File.separator + databaseName;
        try {
            // Create the database storage folder if it doesn't already exist !
            Files.createDirectories(Paths.get(databaseFilePath));
        } catch(IOException ioe) {
            return "Can't seem to create database folder " + databaseFilePath;
        }
        return "OK";
    }

    // Drop a database
    public String deleteDatabase(String databaseName) {
        databaseName = databaseName.toLowerCase();
        String databaseFilePath = storageFolderPath + File.separator + databaseName;

        File databaseToBeDeleted;
        File[] databaseContents;

        try {
            databaseToBeDeleted = new File(databaseFilePath);
            databaseContents = databaseToBeDeleted.listFiles();
        } catch (Exception ioe) {
            return "Error: failed to open database " + databaseName + ". Has it already been deleted?";
        }
        try {
            for (File file : databaseContents) {
                file.delete();
            }
            databaseToBeDeleted.delete();
        } catch (Exception error) {
            return "Error: failed to delete one or more files in database. Error message: " + error;
        }
        return "OK";
    }


    // Create a database table
    public String createDBTable(String databaseName, String tableName) {
        String filePath = this.getFilePath(databaseName, tableName);
        File newTable = new File(filePath + ".tab");

        // Check that database exists
        if (!this.checkFileExists(this.storageFolderPath + File.separator + databaseName)) {
            return "Error: cannot create table in a database that does not exist.";
        }
        // Create empty table - catch errors if permissions are not set
        try {
            newTable.createNewFile();
            return "OK";
        } catch(IOException ioe) {
            return "Error: can't create database table " + filePath + ". Do you have permissions to create files?";
        }
    }

    // Drop a database table


    // Swap database we are interating with


    //  === Methods below handle networking aspects of the project - you will not need to change these ! ===

    public void blockingListenOn(int portNumber) throws IOException {
        try (ServerSocket s = new ServerSocket(portNumber)) {
            System.out.println("Server listening on port " + portNumber);
            while (!Thread.interrupted()) {
                try {
                    blockingHandleConnection(s);
                } catch (IOException e) {
                    System.err.println("Server encountered a non-fatal IO error:");
                    e.printStackTrace();
                    System.err.println("Continuing...");
                }
            }
        }
    }

    private void blockingHandleConnection(ServerSocket serverSocket) throws IOException {
        try (Socket s = serverSocket.accept();
        BufferedReader reader = new BufferedReader(new InputStreamReader(s.getInputStream()));
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(s.getOutputStream()))) {

            System.out.println("Connection established: " + serverSocket.getInetAddress());
            while (!Thread.interrupted()) {
                String incomingCommand = reader.readLine();
                System.out.println("Received message: " + incomingCommand);
                String result = handleCommand(incomingCommand);
                writer.write(result);
                writer.write("\n" + END_OF_TRANSMISSION + "\n");
                writer.flush();
            }
        }
    }
}
