package edu.uob;

// TODO: custom exceptions - identify possible error types
public class DBException extends Exception{

    // TODO: query too short
    // TODO: query does not end with ;
    // TODO: database name/table name/attribute name does not exist
    // TODO:
    public DBException(String message) {
        super(message);
    }

    public static class malformedQueryException extends DBException {
        public malformedQueryException() {
            super("Query is malformed. Please input a valid query.");
        }
    }

    public static class invalidKeywordException extends DBException {
        public invalidKeywordException(String invalidKeyword) {
            super("The keyword " + invalidKeyword + " is invalid. Are you sure you spelled it correctly?");
        }
    }

    public static class invalidIdentifierException extends DBException {
        public invalidIdentifierException(String invalidIdentifier) {
            super("The identifer " + invalidIdentifier + " is invalid. Are you sure you spelled it correctly?");
        }
    }

    public static class noDatabaseSelectedException extends DBException {
        public noDatabaseSelectedException() {
            super("No database has been selected. Please use a 'USE' query to select a database before proceesing.");
        }
    }

    public static class noTerminatorException extends DBException {
        public noTerminatorException() {
            super("Query is invalid. Valid queries must end with a semi-colon (;).");
        }
    }

    public static class queryTooShortException extends DBException {
        public queryTooShortException(int queryLength) {
            super("The query is too short. Query length is " + queryLength + " but queries must contain at least three words (including the semi-colon.");
        }
    }

    public static class databaseNotFoundException extends DBException {
        public databaseNotFoundException(String databaseName) {
            super("The database " + databaseName + " cannot be found. Are you sure it exists?");
        }
    }

    public static class tableNotFoundException extends DBException {
        public tableNotFoundException(String tableName) {
            super("The table " + tableName + " cannot be found. Are you sure it exists?");
        }
    }

    public static class fileNotAccessibleException extends DBException {
        public fileNotAccessibleException(String fileName) {
            super("The file " + fileName + " could not be accessed. Are you sure you have the correct permissions to modify this file?");
        }
    }

    public static class attributeNotInTableException extends DBException {
        public attributeNotInTableException(String attribute) {
            super("This table does not contain the attribute " + attribute + ". Are you sure you spelled the attribute name correctly?");
        }
    }
}
