package edu.uob;

public class DBException extends Exception{

    private static final long serialVersionUID = -11L;

    public DBException(String message) {
        super("[ERROR]: " + message);
    }

    public static class malformedQueryException extends DBException {
        private static final long serialVersionUID = -11L;
        public malformedQueryException() {
            super("Query is malformed. Please input a valid query.");
        }
    }

    public static class invalidKeywordException extends DBException {
        private static final long serialVersionUID = -11L;
        public invalidKeywordException(String invalidKeyword) {
            super("The keyword " + invalidKeyword + " is invalid. Are you sure you spelled it correctly?");
        }
    }

    public static class invalidIdentifierException extends DBException {
        private static final long serialVersionUID = -11L;
        public invalidIdentifierException(String invalidIdentifier) {
            super("Malformed query. The identifier " + invalidIdentifier + " is invalid. Remember that SQL keywords " +
                    "(E.g. Select, Table, Drop, ...) are reserved and forbidden from being used as identifiers!");
        }
    }

    public static class manuallyChangingIdColumnException extends DBException {
        private static final long serialVersionUID = -11L;
        public manuallyChangingIdColumnException() {
            super("Manually changing the 'id' column is forbidden.");
        }
    }

    public static class tableDoesNotExistException extends DBException {
        private static final long serialVersionUID = -11L;
        public tableDoesNotExistException(String tableName) {
            super("Sorry, could not access table " + tableName + " because this table does not exist.");
        }
    }

    public static class databaseDoesNotExistException extends DBException {
        private static final long serialVersionUID = -11L;
        public databaseDoesNotExistException(String databaseName) {
            super("Sorry, could not access database " + databaseName + " because this database does not exist.");
        }
    }
}
