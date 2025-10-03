package edu.uob;

import java.util.*;

import edu.uob.DBException.*;
import edu.uob.Token.*;

public class QueryParser {
    private ArrayList<Token> tokenStream;
    private ParsedQuery parsedQuery;

    public QueryParser(ArrayList<Token> tokenStream) {
        this.tokenStream = tokenStream;
        this.parsedQuery = new ParsedQuery();
    }

    public void parseQuery() throws DBException {
        // Pass query to method depending on commandType
        if (this.tokenStream.get(0).getTokenType() != TokenType.keyword) {
            throw new invalidKeywordException(this.tokenStream.get(0).getTokenValue());
        }
        String commandType = this.tokenStream.get(0).getTokenValue();
        if (commandType.equals("use")) this.parseUse();
        else if (commandType.equals("create")) this.parseCreate();
        else if (commandType.equals("drop")) this.parseDrop();
        else if (commandType.equals("alter")) this.parseAlter();
        else if (commandType.equals("insert")) this.parseInsert();
        else if (commandType.equals("select")) this.parseSelect();
        else if (commandType.equals("update")) this.parseUpdate();
        else if (commandType.equals("delete")) this.parseDelete();
        else if (commandType.equals("join")) this.parseJoin();
    }

    // Check that an identifier is a valid identifier (and not a keyword, etc.)
    private static boolean isValidIdentifier(Token identifier) {
        // Identifier must be a string Literal or plaintext
        if (identifier.getTokenType() != TokenType.stringLiteral &&
                identifier.getTokenType() != TokenType.plainText &&
                identifier.getTokenType() != TokenType.Null) {
            return false;
        }
        return true;
    }

    private static boolean isValidValue(Token value) {
        // Value must be a string Literal between single quotes, a booleanLiteral,
        // a floatLiteral, and integerLiteral or NULL.
        if (value.getTokenType() != TokenType.stringLiteral &&
                value.getTokenType() != TokenType.booleanLiteral &&
                value.getTokenType() != TokenType.floatLiteral &&
                value.getTokenType() != TokenType.integerLiteral &&
                value.getTokenType() != TokenType.Null) {
            return false;
        }
        return true;
    }

    // Getter for Parsed Query
    public ParsedQuery getParsedQuery() {
        return this.parsedQuery;
    }

    private void parseUse() throws DBException {
        // <Use> ::= "USE" [DatabaseName]
        if (this.tokenStream.size() != 3) {
            throw new malformedQueryException();
        }
        if (!isValidIdentifier(this.tokenStream.get(1))) {
            throw new invalidIdentifierException(this.tokenStream.get(1).getTokenValue());
        }
        String databaseName = this.tokenStream.get(1).getTokenValue();

        System.out.println("Using database. databaseName: " + databaseName);

        // Convert results of parser to a ParsedQuery object to be used by QueryHandler
        this.parsedQuery.setCommandType(Keyword.USE);
        this.parsedQuery.setTargetType(TargetType.DATABASE);
        this.parsedQuery.setDatabaseName(databaseName);
        // Handle query
        //QueryHandler handler = new QueryHandler(databaseName);
        //handler.handleUse();
    }

    private void parseCreate() throws DBException {
        // <Create> ::= <>CreateDatabase> | <CreateTable>
        if (this.tokenStream.get(1).getTokenType() != TokenType.keyword) {
            throw new malformedQueryException();
        }
        if (this.tokenStream.get(1).getTokenValue().equals("database")) {
            this.parseCreateDatabase();
        }
        else if (this.tokenStream.get(1).getTokenValue().equals("table")) {
            this.parseCreateTable();
        }
        else {
            throw new invalidKeywordException(this.tokenStream.get(1).getTokenValue());
        }
    }

    private void parseCreateDatabase() throws DBException {
        // <CreateDatabase> ::= "CREATE" "DATABASE" [DatabaseName]
        // Check that DatabaseName is a stringLiteral or plainText identifier
        if (this.tokenStream.size() != 4) {
            throw new malformedQueryException();
        }
        if (!isValidIdentifier(this.tokenStream.get(2))) {
            throw new invalidIdentifierException(this.tokenStream.get(2).getTokenValue());
        }
        String databaseName = this.tokenStream.get(2).getTokenValue();

        System.out.println("Creating database. databaseName: " + databaseName);

        // Convert results of parser to a ParsedQuery object to be used by QueryHandler
        this.parsedQuery.setCommandType(Keyword.CREATE);
        this.parsedQuery.setTargetType(TargetType.DATABASE);
        this.parsedQuery.setDatabaseName(databaseName);
    }

    private void parseCreateTable() throws DBException{
        // <CreateTable> ::= "CREATE" "TABLE" [TableName] | "CREATE" "TABLE" [TableName] "(" <AttributeList> ")"
        if (!isValidIdentifier(this.tokenStream.get(2))) {
            throw new invalidIdentifierException(this.tokenStream.get(2).getTokenValue());
        }
        String tableName = this.tokenStream.get(2).getTokenValue();
        List<String> attributes = new ArrayList<String>();

        if (!this.tokenStream.get(3).getTokenValue().equals(";")) {
            // Includes attributes
            if (!(this.tokenStream.get(3).getTokenValue().equals("(") ||
                    this.tokenStream.get(this.tokenStream.size()-2).getTokenValue().equals(")"))) {
                throw new malformedQueryException();
            }
            attributes = this.parseAttributeList(this.tokenStream.subList(4, this.tokenStream.size()-2));
        }

        System.out.println("Creating table. Table name: " + tableName + ". Attribute list: " + attributes);

        // Convert results of parser to a ParsedQuery object to be used by QueryHandler
        this.parsedQuery.setCommandType(Keyword.CREATE);
        this.parsedQuery.setTargetType(TargetType.TABLE);
        this.parsedQuery.setTableName1(tableName);
        this.parsedQuery.setAttributeList(attributes);


    }

    private ArrayList<String> parseAttributeList(List<Token> attributeList) {
        ArrayList<String> attributes = new ArrayList<String>();
        for (Token attribute : attributeList) {
            if (!attribute.getTokenValue().equals(",")) {
                attributes.add(attribute.getTokenValue());
            }
        }
        return attributes;
    }

    private void parseDrop() throws DBException {
        // <Drop> ::= "DROP" "DATABASE" [DatabaseName] | "DROP" "TABLE" [TableName]
        if (this.tokenStream.size() != 4) {
            throw new malformedQueryException();
        }
        if (!isValidIdentifier(this.tokenStream.get(2))) {
            throw new invalidIdentifierException(this.tokenStream.get(2).getTokenValue());
        }
        if (this.tokenStream.get(1).getTokenValue().equals("database")) {
            this.parseDropDatabase();
        }
        else if (this.tokenStream.get(1).getTokenValue().equals("table")) {
            this.parseDropTable();
        }
        else {
            throw new invalidKeywordException(this.tokenStream.get(1).getTokenValue());
        }
    }

    private void parseDropDatabase() {
        String databaseName = this.tokenStream.get(2).getTokenValue();
        System.out.println("Dropping database. Database name: " + databaseName);

        // Convert results of parser to a ParsedQuery object to be used by QueryHandler
        this.parsedQuery.setCommandType(Keyword.DROP);
        this.parsedQuery.setTargetType(TargetType.DATABASE);
        this.parsedQuery.setDatabaseName(databaseName);
    }

    private void parseDropTable() {
        String tableName = this.tokenStream.get(2).getTokenValue();
        System.out.println("Dropping table. Table name: " + tableName);

        // Convert results of parser to a ParsedQuery object to be used by QueryHandler
        this.parsedQuery.setCommandType(Keyword.DROP);
        this.parsedQuery.setTargetType(TargetType.TABLE);
        this.parsedQuery.setTableName1(tableName);
    }

    private void parseAlter() throws DBException {
        // <Alter> ::= "ALTER" "TABLE" [TableName] " " <AlterationType> " " [AttributeName]
        // [AttributeName] ::= Plaintext
        if (!(this.tokenStream.size() == 6 &&
                this.tokenStream.get(1).getTokenType() == TokenType.keyword &&
                (this.tokenStream.get(3).getTokenType() == TokenType.alterationType)||
                this.tokenStream.get(3).getTokenType() == TokenType.keyword)) {
            throw new malformedQueryException();
        }
        if (!isValidIdentifier(this.tokenStream.get(2))) {
            throw new invalidIdentifierException(this.tokenStream.get(2).getTokenValue());
        }
        if(!isValidIdentifier(this.tokenStream.get(4))) {
            throw new invalidIdentifierException(this.tokenStream.get(4).getTokenValue());
        }

        String tableName = this.tokenStream.get(2).getTokenValue();
        AlterationType alterationType;
        if (this.tokenStream.get(3).getTokenValue().equals("add")) {
            alterationType = AlterationType.ADD;
        } else if (this.tokenStream.get(3).getTokenValue().equals("drop")) {
            alterationType = AlterationType.DROP;
        } else {
            throw new invalidKeywordException(this.tokenStream.get(2).getTokenValue());
        }
        String attribute = this.tokenStream.get(4).getTokenValue();
        List<String> attributeList = Arrays.asList(attribute);
        System.out.println("Altering table. Table name: " + tableName + ". Alteration: " +
                alterationType + ". Attribute to be altered: " + attribute);

        // Convert results of parser to a ParsedQuery object to be used by QueryHandler
        this.parsedQuery.setCommandType(Keyword.ALTER);
        this.parsedQuery.setTargetType(TargetType.TABLE);
        this.parsedQuery.setTableName1(tableName);
        this.parsedQuery.setAlterationType(alterationType);
        this.parsedQuery.setAttributeList(attributeList);
    }

    private void parseInsert() throws DBException {
        // <Insert> ::= "INSERT " "INTO " [TableName] "VALUES " "(" <ValueList> ")"
        if (!(this.tokenStream.get(1).getTokenValue().equals("into") &&
                this.tokenStream.get(3).getTokenValue().equals("values") &&
                this.tokenStream.get(4).getTokenValue().equals("(") &&
                this.tokenStream.get(this.tokenStream.size()-2).getTokenValue().equals(")"))) {
            throw new malformedQueryException();
        }
        if (!(isValidIdentifier(this.tokenStream.get(2)))) {
            throw new invalidIdentifierException(this.tokenStream.get(2).getTokenValue());
        }

        String tableName = this.tokenStream.get(2).getTokenValue();
        ArrayList<String> values = this.parseValueList(this.tokenStream.subList(5, this.tokenStream.size()-2));

        System.out.println("Inserting into table. Table name: " + tableName + ". Value list: " + values);

        // Convert results of parser to a ParsedQuery object to be used by QueryHandler
        this.parsedQuery.setCommandType(Keyword.INSERT);
        this.parsedQuery.setTargetType(TargetType.TABLE);
        this.parsedQuery.setTableName1(tableName);
        this.parsedQuery.setValueList(values);
    }

    private ArrayList<String> parseValueList(List<Token> valueList) {
        ArrayList<String> values = new ArrayList<String>();
        for (Token attribute : valueList) {
            if (!(attribute.getTokenValue().equals(",") || attribute.getTokenValue().equals("'"))) {
                values.add(attribute.getTokenValue());
            }
        }
        return values;
    }

    private static int findToken(List<Token> tokenList, String tokenValue) {
        for (int i = 0; i < tokenList.size(); i++) {
            if (tokenList.get(i).getTokenValue().equals(tokenValue)) return i;
        }
        return -1; // not found
    }

    private void parseSelect() throws DBException {
        // <Select> ::= "SELECT <WildAttributeList> " FROM " [TableName] | "SELECT " <WildAttributeList> "FROM " [TableName] " WHERE " <Condition>
        int endOfAttributeList = findToken(this.tokenStream, "from");
        int conditionStartIndex = findToken(this.tokenStream, "where") + 1;
        // Check query is valid
        if (!(this.tokenStream.get(endOfAttributeList).getTokenValue().equals("from") &&
                isValidIdentifier(this.tokenStream.get(endOfAttributeList+1)))) {
            throw new invalidIdentifierException(this.tokenStream.get(endOfAttributeList+1).getTokenValue());
        }

        String tableName = this.tokenStream.get(endOfAttributeList+1).getTokenValue();
        // Parse <WildAttributeList>
        ArrayList<String> attributes = this.parseAttributeList(this.tokenStream.subList(1, endOfAttributeList));

        // Parse "WHERE" <Condition> (if present)
        Condition conditions;
        if (conditionStartIndex > 0) {
            conditions = parseConditions(this.tokenStream.subList(endOfAttributeList + 3, this.tokenStream.size() - 1));
            this.parsedQuery.setCondition(conditions);
        }

        // For testing only
        if (conditionStartIndex > 0) {
            //ArrayList<String> printConditions = new ArrayList<String>();
            //for (int i = 0; i < conditions.size(); i++) {
            //    printConditions.add(conditions.get(i).getTokenValue());
            //}

            //System.out.println("Selecting table. Table name: " + tableName + ". Attribute list: " +
            //        attributes + ". Conditions: " + conditions);
        } else {
            System.out.println("Selecting table. Table name: " + tableName + ". Attribute list: " +
                    attributes);
        }

        // Convert results of parser to a ParsedQuery object to be used by QueryHandler
        this.parsedQuery.setCommandType(Keyword.SELECT);
        this.parsedQuery.setTargetType(TargetType.TABLE);
        this.parsedQuery.setTableName1(tableName);
        this.parsedQuery.setAttributeList(attributes);

    }

    private static List<Token> removeBracketsFromCondition(List<Token> tokens) {
        for (int i = 0; i < tokens.size(); i++) {
            if (tokens.get(i).getTokenValue().equals("(") ||
                    tokens.get(i).getTokenValue().equals(")")) {
                tokens.remove(i);
            }
        }
        return tokens;
    }

    private static Condition parseConditions(List<Token> conditionTokens) throws DBException {
        // Parse "WHERE" <Condition> (if present)
        //List<Token> conditions = null;
        //if (!conditionTokens.get(0).getTokenValue().equals("where")) {
        //    return conditionTokens;
        //}
        //List<Condition> conditions = parseCondition(this.tokenStream.subList(endOfAttributeList+3, this.tokenStream.size()-1));
        //return conditionTokens.subList(1, conditionTokens.size());

        // Remove brackets, if any
        conditionTokens = removeBracketsFromCondition(conditionTokens);

        // Top-level Condition object
        Condition topCondition = new Condition();
        parseNonTerminalCondition(topCondition, conditionTokens);
        return topCondition;
    }

    private static void parseNonTerminalCondition(Condition condition, List<Token> tokens) 
    throws DBException {
        int boolOperatorIndex = indexOfBooleanOperator(tokens);
        if (boolOperatorIndex == -1) {
            parseTerminalCondition(condition, tokens);
            return;
        }

        Condition condition1 = new Condition();
        Condition condition2 = new Condition();
        List<Token> tokensSublist1 = tokens.subList(0, boolOperatorIndex);
        List<Token> tokensSublist2 = tokens.subList(boolOperatorIndex+1, tokens.size());
        parseNonTerminalCondition(condition1, tokensSublist1);
        parseNonTerminalCondition(condition2, tokensSublist2);
        condition.setCondition1(condition1);
        condition.setCondition2(condition2);

        BooleanOperator booleanOperator;
        if (tokens.get(boolOperatorIndex).getTokenValue().equals("and")) {
            booleanOperator = BooleanOperator.AND;
        } else if (tokens.get(boolOperatorIndex).getTokenValue().equals("or")) {
            booleanOperator = BooleanOperator.OR;
        } else {
            throw new DBException("Malformed condition.");
        }
        condition.setBooleanOperator(booleanOperator);
    }

    private static void parseTerminalCondition(Condition condition, List<Token> tokens)
    throws DBException {
        List<String> comparators = Arrays.asList("==", ">", "<", ">=", "<=", "!=", "like");
        int boolOperatorIndex = indexOfBooleanOperator(tokens);
        // No boolean operator means terminal condition -> [attributeName] [comparator] [value]
        if (tokens.size() != 3 ||
                !comparators.contains(tokens.get(1).getTokenValue())){
            throw new DBException("Malformed condition");
        }
        condition.setAttributeName(tokens.get(0).getTokenValue());
        condition.setComparator(tokens.get(1).getTokenValue());
        condition.setValue(tokens.get(2).getTokenValue());
    }

    private static int indexOfBooleanOperator(List<Token> tokens) {
        List<String> booleanOperators = Arrays.asList("and", "or");
        for (int i = 0; i < tokens.size(); i++) {
            for (String operator: booleanOperators) {
                if (tokens.get(i).getTokenValue().equals(operator)) return i;
            }
        }
        return -1; // -1 means not found
    }

    /*
    public class Condition {
        public String attributeName;
        public String comparator;
        public String value;

        public Condition(List<Token> conditionTokens) {
            this.attributeName = conditionTokens.get(0).getTokenValue();
            this.comparator = conditionTokens.get(1).getTokenValue();
            this.value = conditionTokens.get(2).getTokenValue();
        }
    }


    private List<Condition> parseConditionList(List<Token> conditionTokens) {
        // <Condition> ::= "( TODO
        // If it contains a booleanOperator, it recurses (multiple conditions)
        List<Condition> conditions = new List<Condition>();
        int andIndex = findToken("and");
        int orIndex = findToken("or");
        while (andIndex != -1) {
            conditions.add(parseCondition(conditionTokens.subList(0, andIndex)));
        }
    }

    private Condition parseCondition(List<Token> conditionTokens) {
        // <Condition> ::= "( TODO
        return new Condition(conditionTokens);
    }
     */

    private void parseUpdate() throws DBException {
        // <Update> ::= "UPDATE " [TableName] " SET " <NameValueList> " WHERE " <Condition>
        int endOfNameValueList = findToken(this.tokenStream, "where");

        if (!(isValidIdentifier(this.tokenStream.get(1)))) {
            throw new invalidIdentifierException(this.tokenStream.get(1).getTokenValue());
        }
        if (!(this.tokenStream.get(2).getTokenValue().equals("set") &&
                endOfNameValueList != -1)) {
            throw new malformedQueryException();
        }

        String tableName = this.tokenStream.get(1).getTokenValue();

        // Parse <NameValueList>
        ArrayList<NameValuePair> nameValueList = this.parseNameValueList(this.tokenStream.subList(3, endOfNameValueList));

        // Parse "WHERE" <Condition> (if present)
        //List<Token> conditions = this.tokenStream.subList(endOfNameValueList+1, this.tokenStream.size()-1);
        Condition conditions = parseConditions(this.tokenStream.subList(endOfNameValueList+1, this.tokenStream.size()-1));

        // For testing only
        /*ArrayList<String> printConditions = new ArrayList<String>();
        for (int i = 0; i < conditions.size(); i++) {
            printConditions.add(conditions.get(i).getTokenValue());
        }

         */

        ArrayList<String> printnamevalues = new ArrayList<String>();
        for (int i = 0; i < nameValueList.size(); i++) {
            printnamevalues.add(nameValueList.get(i).getAttributeName() + " : " + nameValueList.get(i).getValue());
        }
        System.out.println("Updating table. Table name: " + tableName + ". NameValue list: " +
                printnamevalues + ". Conditions: " + conditions);

        // Convert results of parser to a ParsedQuery object to be used by QueryHandler
        this.parsedQuery.setCommandType(Keyword.UPDATE);
        this.parsedQuery.setTargetType(TargetType.TABLE);
        this.parsedQuery.setTableName1(tableName);
        this.parsedQuery.setNameValueList(nameValueList);
        this.parsedQuery.setCondition(conditions);

    }

    private ArrayList<NameValuePair> parseNameValueList(List<Token> nameValueList) throws DBException {
        // <NameValueList> ::= <NameValuePair> | <NameValuePair> "," <NameValueList>

        ArrayList<NameValuePair> nameValues = new ArrayList<NameValuePair>();
        int nameValuePairSeparator = findToken(nameValueList, ",");
        while (nameValuePairSeparator != -1) {
            nameValues.add(parseNameValuePair(nameValueList.subList(0, nameValuePairSeparator)));
            nameValueList = nameValueList.subList(nameValuePairSeparator+1, nameValueList.size());
            nameValuePairSeparator = findToken(nameValueList, ",");
        }
        nameValues.add(parseNameValuePair(nameValueList));

        return nameValues;
    }

    private NameValuePair parseNameValuePair(List<Token> nameValuePair) throws DBException{
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

    private void parseDelete() throws DBException {
        // <Delete> ::= "DELETE " "FROM " [TableName] " WHERE " <Condition>
        if (!(this.tokenStream.get(1).getTokenValue().equals("from") &&
                this.tokenStream.get(3).getTokenValue().equals("where"))) {
            throw new malformedQueryException();
        }
        if (!(isValidIdentifier(this.tokenStream.get(2)))) {
            throw new invalidIdentifierException(this.tokenStream.get(2).getTokenValue());
        }

        int conditionStartIndex = findToken(this.tokenStream, "where") + 1;
        String tableName = this.tokenStream.get(2).getTokenValue();
        Condition conditions = parseConditions(this.tokenStream.subList(conditionStartIndex, this.tokenStream.size()-1));

        // For testing only
        /*ArrayList<String> printConditions = new ArrayList<String>();
        for (int i = 0; i < conditions.size(); i++) {
            printConditions.add(conditions.get(i).getTokenValue());
        }

         */

        System.out.println("Deleting from table. Table name: " + tableName +
                ". Conditions: " + conditions);

        // Convert results of parser to a ParsedQuery object to be used by QueryHandler
        this.parsedQuery.setCommandType(Keyword.DELETE);
        this.parsedQuery.setTargetType(TargetType.TABLE);
        this.parsedQuery.setTableName1(tableName);
        this.parsedQuery.setCondition(conditions);

    }

    private void parseJoin() throws DBException{
        // <Join> ::= "JOIN "  [TableName] " " AND " [TableName] " ON " [AttributeName] " AND " [AttributeName]
        if (!(this.tokenStream.size() == 9 &&
                this.tokenStream.get(2).getTokenValue().equals("and") &&
                this.tokenStream.get(4).getTokenValue().equals("on") &&
                this.tokenStream.get(6).getTokenValue().equals("and"))) {
            throw new malformedQueryException();
        }
        if (!isValidIdentifier(this.tokenStream.get(1))) throw new invalidIdentifierException(this.tokenStream.get(1).getTokenValue());
        if (!isValidIdentifier(this.tokenStream.get(3))) throw new invalidIdentifierException(this.tokenStream.get(3).getTokenValue());
        if (!isValidIdentifier(this.tokenStream.get(5))) throw new invalidIdentifierException(this.tokenStream.get(5).getTokenValue());
        if (!isValidIdentifier(this.tokenStream.get(7))) throw new invalidIdentifierException(this.tokenStream.get(7).getTokenValue());

        // Get table names
        String tableName1 = this.tokenStream.get(1).getTokenValue();
        String tableName2 = this.tokenStream.get(3).getTokenValue();

        // Get attribute names
        String attribute1 = this.tokenStream.get(5).getTokenValue();
        String attribute2 = this.tokenStream.get(7).getTokenValue();

        System.out.println("Joining tables " + tableName1 + " and " + tableName2 +
                " on attributes: "  + attribute1 + " and " + attribute2);

        // Convert results of parser to a ParsedQuery object to be used by QueryHandler
        this.parsedQuery.setCommandType(Keyword.JOIN);
        this.parsedQuery.setTargetType(TargetType.TABLE);
        this.parsedQuery.setTableName1(tableName1);
        this.parsedQuery.setTableName2(tableName2);
        this.parsedQuery.setJoinAttribute1(attribute1);
        this.parsedQuery.setJoinAttribute2(attribute2);
    }
}
