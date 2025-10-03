package edu.uob;

import java.io.*;
import java.util.*;
import java.nio.file.Paths;
import java.util.ArrayList;
import edu.uob.DBException.*;
import edu.uob.Token.*;

public abstract class DBcmd {
    protected static String databaseName;
    protected static String databasePath;
    protected ArrayList<Token> tokenStream;

    protected TargetType targetType;
    protected String tableName;
    protected String tablePath;
    protected AlterationType alterationType;
    protected List<String> attributeList;
    protected Condition condition;
    protected ArrayList<String> valueList;
    protected ArrayList<NameValuePair> nameValueList;
    protected DBTable databaseTable;


    public DBcmd() {
    }

    public abstract String handleQuery();

    public abstract void parseCommand() throws DBException;

    public void setDatabaseName(String dbName) {
        databaseName = dbName;
        databasePath = this.getFilePath(null);
    }


    // The following functions do error checking/handling for parse methods.
    // Check that an identifier is a valid identifier (and not a keyword, etc.)
    protected static boolean isValidIdentifier(Token identifier) {
        // Identifier must be a string Literal or plaintext
        if (identifier.getTokenType() != Token.TokenType.stringLiteral &&
                identifier.getTokenType() != Token.TokenType.plainText &&
                identifier.getTokenType() != Token.TokenType.Null) {
            return false;
        }
        // Identifier must be composed ONLY of letters and digits
        return identifier.getTokenValue().matches("[a-zA-Z0-9]*");
    }


    protected static boolean isValidValue(Token value) {
        // Value must be a string Literal between single quotes, a booleanLiteral,
        // a floatLiteral, and integerLiteral or NULL.
        return value.getTokenType() == Token.TokenType.stringLiteral ||
                value.getTokenType() == Token.TokenType.booleanLiteral ||
                value.getTokenType() == Token.TokenType.floatLiteral ||
                value.getTokenType() == Token.TokenType.integerLiteral ||
                value.getTokenType() == Token.TokenType.Null;
    }


    protected void checkIdentifier(int tokenIndex) throws DBException {
        if (!isValidIdentifier(this.tokenStream.get(tokenIndex))) {
            throw new invalidIdentifierException(this.tokenStream.get(tokenIndex).getTokenValue());
        }
    }

    protected void checkKeyword(int tokenIndex, String expectedKeyword) throws DBException {
        if (!this.tokenStream.get(tokenIndex).getTokenValue().equals(expectedKeyword)) {
            throw new malformedQueryException();
        }
    }


    protected void checkTokenType(int tokenIndex, TokenType expectedTokenType) throws DBException {
        if (!(this.tokenStream.get(tokenIndex).getTokenType() == expectedTokenType)) {
            throw new malformedQueryException();
        }
    }


    protected void checkQueryLength(int expectedLength) 
    throws DBException {
        if (!(tokenStream.size() == expectedLength)) {
            throw new malformedQueryException();
        }
    }


    // Utility function for parsing attribute lists
    protected void parseAttributeList(List<Token> attributeList) {
        ArrayList<String> attributes = new ArrayList<>();
        for (Token attribute : attributeList) {
            if (!attribute.getTokenValue().equals(",")) {
                attributes.add(attribute.getTokenValue());
            }
        }
        this.attributeList = attributes;
    }


    // Utility function for parsing value lists
    protected void parseValueList(List<Token> valueList) {
        ArrayList<String> values = new ArrayList<>();
        for (Token attribute : valueList) {
            if (!(attribute.getTokenValue().equals(",") || attribute.getTokenValue().equals("'"))) {
                values.add(attribute.getTokenValue());
            }
        }
        this.valueList = values;
    }


    // Returns the index of the input token in a token list.
    // If search token not in list, returns -1
    protected static int findToken(List<Token> tokenList, String tokenValue) {
        for (int i = 0; i < tokenList.size(); i++) {
            if (tokenList.get(i).getTokenValue().equals(tokenValue)) return i;
        }
        return -1; // not found
    }


    // Utility functions for parsing Conditions
    protected static void removeBrackets(List<Token> tokens) {
        tokens.removeIf(token -> "(".equals(token.getTokenValue()) || ")".equals(token.getTokenValue()));
    }


    // Parses terminal conditions that contains no whitespace between values
    // and comparator e.g. age<40.
    protected static List<Token> parseConditionNoWhitespace(List<Token> tokens) {
        // If no spaces between condition tokens, separate them out
        List<String> comparators = Arrays.asList("==", ">", "<", ">=", "<=", "!=");
        List<Token> parsedTokens = new ArrayList<>(tokens);
        for (Token token: tokens) {
            for (String comparator : comparators) {
                if (!token.getTokenValue().equals(comparator) &&
                        token.getTokenValue().contains(comparator)) {
                    int conditionIndex = tokens.indexOf(token);
                    String[] tokenValues = token.getTokenValue().split(comparator);
                    parsedTokens.remove(conditionIndex);
                    parsedTokens.add(conditionIndex, new Token(tokenValues[0]));
                    parsedTokens.add(conditionIndex+1, new Token(comparator));
                    parsedTokens.add(conditionIndex+2, new Token(tokenValues[1]));
                    break;
                }
            }
        }
        return parsedTokens;
    }


    protected void parseConditions(List<Token> conditionTokens) throws DBException {
        // If no condition, just return
        if (conditionTokens.isEmpty()) return;

        // If no spaces between condition tokens, separate them out
        conditionTokens = parseConditionNoWhitespace(conditionTokens);

        // Top-level Condition object
        Condition topCondition = new Condition();
        parseNonTerminalCondition(topCondition, conditionTokens);
        this.condition = topCondition;
    }


    // If there are brackets in the condition, correctly apply precedence rules. I.e. top-level condition
    // is the last to be evaluated, so conditions most deeply nested in brackets are most deeply nested in
    // recursive condition structure.
    private static int evaluatePrecedence(List<Token> tokens, int boolOperatorIndex) {
        int openBracketIndex = findToken(tokens, "(");
        int closeBracketIndex = findToken(tokens, ")");

        // Precedence - evaluate condition within the brackets (if they are present) first.
        if (openBracketIndex != -1 && closeBracketIndex != -1) {
            // Is there a boolean operator outside the brackets? If so, this is top-level bool operator
            while (boolOperatorIndex != -1 && boolOperatorIndex > openBracketIndex &&
                    boolOperatorIndex < closeBracketIndex) {
                boolOperatorIndex = indexOfBooleanOperator(tokens.subList(closeBracketIndex+1,
                        tokens.size()))+closeBracketIndex+1;
            }
            // If gets to end of loop and boolOperatorIndex == -1, means that the only boolOperator is
            // inside the brackets - use this one. Else, start with the boolOperator outside the bracket
            if (boolOperatorIndex == -1 || !(tokens.get(boolOperatorIndex).getTokenValue().equals("and") ||
                    tokens.get(boolOperatorIndex).getTokenValue().equals("or"))) {
                boolOperatorIndex = indexOfBooleanOperator(tokens);
            }
        }
        return boolOperatorIndex;
    }


    protected void parseNonTerminalCondition(Condition condition, List<Token> tokens)
            throws DBException {
        int boolOperatorIndex = indexOfBooleanOperator(tokens);
        if (boolOperatorIndex == -1) {
            parseTerminalCondition(condition, tokens);
            return;
        }
        Condition condition1 = new Condition();
        Condition condition2 = new Condition();
        // Precedence - evaluate condition within the brackets (if they are present) first.
        boolOperatorIndex = evaluatePrecedence(tokens, boolOperatorIndex);

        // Recursively evaluate non-terminal conditions within this condition
        List<Token> tokensSublist1 = new ArrayList<>(tokens.subList(0, boolOperatorIndex));
        List<Token> tokensSublist2 = new ArrayList<>(tokens.subList(boolOperatorIndex + 1, tokens.size()));
        parseNonTerminalCondition(condition1, tokensSublist1);
        parseNonTerminalCondition(condition2, tokensSublist2);
        condition.setCondition1(condition1);
        condition.setCondition2(condition2);

        BooleanOperator booleanOperator = getBooleanOperator(tokens, boolOperatorIndex);
        condition.setBooleanOperator(booleanOperator);
    }


    private static BooleanOperator getBooleanOperator(List<Token> tokens, int boolOperatorIndex)
            throws DBException {
        BooleanOperator booleanOperator;
        if (tokens.get(boolOperatorIndex).getTokenValue().equals("and")) {
            booleanOperator = BooleanOperator.AND;
        } else if (tokens.get(boolOperatorIndex).getTokenValue().equals("or")) {
            booleanOperator = BooleanOperator.OR;
        } else {
            throw new DBException("Malformed condition.");
        }
        return booleanOperator;
    }


    protected static void parseTerminalCondition(Condition condition, List<Token> tokens)
            throws DBException {
        // Remove brackets
        removeBrackets(tokens);
        List<String> comparators = Arrays.asList("==", ">", "<", ">=", "<=", "!=", "like");
        // No boolean operator means terminal condition -> [attributeName] [comparator] [value]
        if (tokens.size() != 3 ||
                !comparators.contains(tokens.get(1).getTokenValue())){
            throw new DBException("Malformed condition");
        }
        condition.setAttributeName(tokens.get(0).getTokenValue());
        condition.setComparator(tokens.get(1).getTokenValue());
        condition.setValue(tokens.get(2).getTokenValue());
    }


    protected static int indexOfBooleanOperator(List<Token> tokens) {
        List<String> booleanOperators = Arrays.asList("and", "or");
        for (int i = 0; i < tokens.size(); i++) {
            for (String operator: booleanOperators) {
                if (tokens.get(i).getTokenValue().equals(operator)) return i;
            }
        }
        return -1; // -1 means not found
    }


    // Returns a list of the ids of columns to be deleted/updated (where condition is true)
    protected ArrayList<String> filterTableByCondition()
            throws DBException {
        ArrayList<String> filteredDataIds = this.databaseTable.filterRows(this.condition);
        // If WHERE condition is never true, i.e. the above returns an empty list, error
        if (filteredDataIds.isEmpty()) {
            throw new DBException("No rows in the table matched the condition. Nothing was changed.");
        }
        return filteredDataIds;
    }


    // Utility functions to parse name value pairs/lists
    protected void parseNameValueList(List<Token> nameValueList)
    throws DBException {
        // <NameValueList> ::= <NameValuePair> | <NameValuePair> "," <NameValueList>
        ArrayList<NameValuePair> nameValues = new ArrayList<>();
        int nameValuePairSeparator = findToken(nameValueList, ",");
        while (nameValuePairSeparator != -1) {
            nameValues.add(this.parseNameValuePair(nameValueList.subList(0, nameValuePairSeparator)));
            nameValueList = nameValueList.subList(nameValuePairSeparator+1, nameValueList.size());
            nameValuePairSeparator = findToken(nameValueList, ",");
        }
        nameValues.add(this.parseNameValuePair(nameValueList));
        this.nameValueList = nameValues;
    }


    protected NameValuePair parseNameValuePair(List<Token> nameValuePair) throws DBException{
        // <NameValuePair> ::= [AttributeName] "=" [Value]
        if (nameValuePair.size() < 3) {
            throw new DBException("Malformed name value pair.");
        }

        if (!(isValidIdentifier(nameValuePair.get(0)) &&
                nameValuePair.get(1).getTokenValue().equals("=") &&
                isValidValue(nameValuePair.get(2)))) {
            throw new malformedQueryException();
        }
        String attributeName = nameValuePair.get(0).getTokenValue();
        String value = nameValuePair.get(2).getTokenValue();
        return new NameValuePair(attributeName, value);
    }


    // Utility functions for file I/O and database table instantiation
    protected String getFilePath(String tableName) {
        String storageFolderPath = Paths.get("databases").toAbsolutePath().toString();
        String filePath = storageFolderPath + File.separator + databaseName;
        if (tableName != null) {
            return filePath + File.separator + tableName + ".tab";
        }
        return filePath;
    }


    protected static boolean checkFileExists(String filePath) {
        File fileToBeChecked = new File(filePath);
        return fileToBeChecked.exists();
    }


    protected void checkDatabaseExists() throws DBException {
        // Check that database exists
        if (!checkFileExists(databasePath)) {
            throw new DBException("Sorry, this database does not exist.");
        }
    }


    protected void checkTableExists() throws DBException {
        // Check that database exists
        if (!checkFileExists(this.tablePath)) {
            throw new tableDoesNotExistException(this.tableName);
        }
    }


    // Append text onto an existing file (used to write new table rows to metadata file)
    protected static void appendToFile(String filePath, String text) throws DBException {
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, true));
            writer.write("\n");
            writer.write(text);
            writer.close();
        } catch (IOException e) {
            throw new DBException("Failed to write to file" + filePath);
        }
    }


    protected void instantiateDatabaseTable()
            throws DBException {
        this.checkTableExists();
        this.databaseTable = new DBTable(this.tablePath);
    }
}