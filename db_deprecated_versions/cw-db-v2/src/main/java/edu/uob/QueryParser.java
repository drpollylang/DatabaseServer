package edu.uob;

import java.util.*;

import edu.uob.DBException.*;
import edu.uob.Token.*;

public class QueryParser {
    private ArrayList<Token> tokenStream;

    public QueryParser(ArrayList<Token> tokenStream) {
        this.tokenStream = tokenStream;
    }

    public DBcmd parseQuery() throws DBException {
        // Pass query to method depending on commandType
        if (this.tokenStream.get(0).getTokenType() != TokenType.keyword) {
            throw new invalidKeywordException(this.tokenStream.get(0).getTokenValue());
        }
        this.convertTokenStreamToLower();

        String commandType = this.tokenStream.get(0).getTokenValue();
        return switch (commandType) {
            case "use" -> this.parseUse();
            case "create" -> this.parseCreate();
            case "drop" -> this.parseDrop();
            case "alter" -> this.parseAlter();
            case "insert" -> this.parseInsert();
            case "select" -> this.parseSelect();
            case "update" -> this.parseUpdate();
            case "delete" -> this.parseDelete();
            case "join" -> this.parseJoin();
            default -> throw new DBException("Error: invalid type of command. Please input a valid query.");
        };
    }


    // Check that an identifier is a valid identifier (and not a keyword, etc.)
    private static boolean isValidIdentifier(Token identifier) {
        // Identifier must be a string Literal or plaintext
        if (identifier.getTokenType() != TokenType.stringLiteral &&
                identifier.getTokenType() != TokenType.plainText &&
                identifier.getTokenType() != TokenType.Null) {
            return false;
        }
        // Identifier must be composed ONLY of letters and digits
        return identifier.getTokenValue().matches("[a-zA-Z0-9]*");
    }


    private static boolean isValidValue(Token value) {
        // Value must be a string Literal between single quotes, a booleanLiteral,
        // a floatLiteral, and integerLiteral or NULL.
        return value.getTokenType() == TokenType.stringLiteral ||
                value.getTokenType() == TokenType.booleanLiteral ||
                value.getTokenType() == TokenType.floatLiteral ||
                value.getTokenType() == TokenType.integerLiteral ||
                value.getTokenType() == TokenType.Null;
    }


    private void convertTokenStreamToLower() {
        // This has to do with making queries case-insensitive and converting
        // table/database names to lowercase, while preserving case in attribute names
        // Converts all of the tokens EXCEPT column names to lowercase.
        // This proviso only pertains to the Alter, Update and Create commands
        int lastIndex;

        if (this.tokenStream.get(0).getTokenValue().equalsIgnoreCase("alter")) {
            lastIndex = this.tokenStream.size() - 2;
        } else if (this.tokenStream.get(0).getTokenValue().equalsIgnoreCase("create") &&
                this.tokenStream.size() > 3) {
            lastIndex = 3;
        } else if (this.tokenStream.get(0).getTokenValue().equalsIgnoreCase("insert")) {
            lastIndex = 4;
        } else {
            lastIndex = this.tokenStream.size();
        }
        for (int i = 0; i < lastIndex; i++) {
            if (!(i > 2 && this.tokenStream.get(i - 1).getTokenValue().equals("=") &&
                    this.tokenStream.get(0).getTokenValue().equals("update"))) {
                this.tokenStream.get(i).setTokenToLowerCase();
            }
        }

    }


    private DBcmd parseUse() throws DBException {
        // <Use> ::= "USE" [DatabaseName]
        if (this.tokenStream.size() != 3) {
            throw new malformedQueryException();
        }
        if (!isValidIdentifier(this.tokenStream.get(1))) {
            throw new invalidIdentifierException(this.tokenStream.get(1).getTokenValue());
        }
        String databaseName = this.tokenStream.get(1).getTokenValue();
        return new UseCMD(databaseName);
    }


    private DBcmd parseCreate() throws DBException {
        // <Create> ::= <>CreateDatabase> | <CreateTable>
        if (this.tokenStream.get(1).getTokenType() != TokenType.keyword) {
            throw new malformedQueryException();
        }
        if (this.tokenStream.get(1).getTokenValue().equals("database")) {
            return this.parseCreateDatabase();
        }
        else if (this.tokenStream.get(1).getTokenValue().equals("table")) {
            return this.parseCreateTable();
        }
        else {
            throw new DBException("Malformed query. The second word of a CREATE query should " +
                    "be either DATABASE or TABLE.");
        }
    }


    private DBcmd parseCreateDatabase() throws DBException {
        // <CreateDatabase> ::= "CREATE" "DATABASE" [DatabaseName]
        // Check that DatabaseName is a stringLiteral or plainText identifier
        if (this.tokenStream.size() != 4) {
            throw new malformedQueryException();
        }
        if (!isValidIdentifier(this.tokenStream.get(2))) {
            throw new invalidIdentifierException(this.tokenStream.get(2).getTokenValue());
        }
        String databaseName = this.tokenStream.get(2).getTokenValue();
        return new CreateCMD(TargetType.DATABASE, databaseName, null);
    }


    private DBcmd parseCreateTable() throws DBException{
        // <CreateTable> ::= "CREATE" "TABLE" [TableName] | "CREATE" "TABLE" [TableName] "(" <AttributeList> ")"
        if (!isValidIdentifier(this.tokenStream.get(2))) {
            throw new invalidIdentifierException(this.tokenStream.get(2).getTokenValue());
        }
        String tableName = this.tokenStream.get(2).getTokenValue();
        List<String> attributes = new ArrayList<>();

        if (!this.tokenStream.get(3).getTokenValue().equals(";")) {
            // Includes attributes
            if (!(this.tokenStream.get(3).getTokenValue().equals("(") ||
                    this.tokenStream.get(this.tokenStream.size()-2).getTokenValue().equals(")"))) {
                throw new malformedQueryException();
            }
            attributes = this.parseAttributeList(this.tokenStream.subList(4, this.tokenStream.size()-2));
        }
        return new CreateCMD(TargetType.TABLE, tableName, attributes);
    }


    private ArrayList<String> parseAttributeList(List<Token> attributeList) {
        ArrayList<String> attributes = new ArrayList<>();
        for (Token attribute : attributeList) {
            if (!attribute.getTokenValue().equals(",")) {
                attributes.add(attribute.getTokenValue());
            }
        }
        return attributes;
    }


    private DBcmd parseDrop() throws DBException {
        // <Drop> ::= "DROP" "DATABASE" [DatabaseName] | "DROP" "TABLE" [TableName]
        if (this.tokenStream.size() != 4) {
            throw new malformedQueryException();
        }
        if (!isValidIdentifier(this.tokenStream.get(2))) {
            throw new invalidIdentifierException(this.tokenStream.get(2).getTokenValue());
        }
        if (this.tokenStream.get(1).getTokenValue().equals("database")) {
            return this.parseDropDatabase();
        }
        else if (this.tokenStream.get(1).getTokenValue().equals("table")) {
            return this.parseDropTable();
        }
        else {
            throw new DBException("Malformed query. The second word of a DROP query should be either " +
                    "DATABASE or TABLE, but you input " + this.tokenStream.get(1).getTokenValue());
        }
    }


    private DBcmd parseDropDatabase() {
        String databaseName = this.tokenStream.get(2).getTokenValue();
        return new DropCMD(TargetType.DATABASE, databaseName);
    }


    private DBcmd parseDropTable() {
        String tableName = this.tokenStream.get(2).getTokenValue();
        return new DropCMD(TargetType.TABLE, tableName);
    }

    private DBcmd parseAlter() throws DBException {
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
        AlterationType alterationType = getAlterationType();
        String attribute = this.tokenStream.get(4).getTokenValue();
        List<String> attributeList = Arrays.asList(attribute);

        return new AlterCMD(tableName, alterationType, attributeList);
    }


    private AlterationType getAlterationType() throws DBException {
        AlterationType alterationType;
        if (this.tokenStream.get(3).getTokenValue().equals("add")) {
            alterationType = AlterationType.ADD;
        } else if (this.tokenStream.get(3).getTokenValue().equals("drop")) {
            alterationType = AlterationType.DROP;
        } else {
            throw new DBException("Malformed query. The third word of an ALTER query should be " +
                    "either ADD or DROP, but you input " + this.tokenStream.get(2).getTokenValue());
        }
        return alterationType;
    }


    private DBcmd parseInsert() throws DBException {
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

        return new InsertCMD(tableName, values);
    }


    private ArrayList<String> parseValueList(List<Token> valueList) {
        ArrayList<String> values = new ArrayList<>();
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


    private DBcmd parseSelect() throws DBException {
        // <Select> ::= "SELECT <WildAttributeList> " FROM " [TableName] | "SELECT "
        // <WildAttributeList> "FROM " [TableName] " WHERE " <Condition>
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
        Condition conditions = null;
        if (conditionStartIndex > 0) {
            conditions = parseConditions(this.tokenStream.subList(endOfAttributeList + 3, this.tokenStream.size() - 1));
        }

        return new SelectCMD(tableName, conditions, attributes);
    }


    private static void removeBracketsFromCondition(List<Token> tokens) {
        tokens.removeIf(token -> "(".equals(token.getTokenValue()) || ")".equals(token.getTokenValue()));
    }

    // Parses terminal conditions that contains no whitespace between values
    // and comparator e.g. age<40.
    private static List<Token> parseConditionWithoutWhitespace(List<Token> tokens) {
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


    private static Condition parseConditions(List<Token> conditionTokens) throws DBException {
        // If no spaces between condition tokens, separate them out
        conditionTokens = parseConditionWithoutWhitespace(conditionTokens);

        // Top-level Condition object
        Condition topCondition = new Condition();
        parseNonTerminalCondition(topCondition, conditionTokens);
        return topCondition;
    }


    private static void parseNonTerminalCondition(Condition condition, List<Token> tokens) 
    throws DBException {
        int openBracketIndex = findToken(tokens, "(");
        int closeBracketIndex = findToken(tokens, ")");
        int boolOperatorIndex = indexOfBooleanOperator(tokens);
        if (boolOperatorIndex == -1) {
            parseTerminalCondition(condition, tokens);
            return;
        }
        Condition condition1 = new Condition();
        Condition condition2 = new Condition();
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
        List<Token> tokensSublist1 = new ArrayList<>(tokens.subList(0, boolOperatorIndex));
        List<Token> tokensSublist2 = new ArrayList<>(tokens.subList(boolOperatorIndex + 1, tokens.size()));
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
        // Remove brackets
        removeBracketsFromCondition(tokens);
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


    private static int indexOfBooleanOperator(List<Token> tokens) {
        List<String> booleanOperators = Arrays.asList("and", "or");
        for (int i = 0; i < tokens.size(); i++) {
            for (String operator: booleanOperators) {
                if (tokens.get(i).getTokenValue().equals(operator)) return i;
            }
        }
        return -1; // -1 means not found
    }


    private DBcmd parseUpdate() throws DBException {
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
        ArrayList<NameValuePair> nameValueList = this.parseNameValueList(this.tokenStream.subList(3,
                endOfNameValueList));

        // Parse "WHERE" <Condition> (if present)
        Condition conditions = parseConditions(this.tokenStream.subList(endOfNameValueList+1,
                this.tokenStream.size()-1));

        return new UpdateCMD(tableName, conditions, nameValueList);

    }


    private ArrayList<NameValuePair> parseNameValueList(List<Token> nameValueList) throws DBException {
        // <NameValueList> ::= <NameValuePair> | <NameValuePair> "," <NameValueList>
        ArrayList<NameValuePair> nameValues = new ArrayList<>();
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


    private DBcmd parseDelete() throws DBException {
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
        Condition conditions = parseConditions(this.tokenStream.subList(conditionStartIndex,
                this.tokenStream.size()-1));

        return new DeleteCMD(tableName, conditions);
    }


    private DBcmd parseJoin() throws DBException{
        // <Join> ::= "JOIN "  [TableName] " " AND " [TableName] " ON " [AttributeName] "
        // AND " [AttributeName]
        if (!(this.tokenStream.size() == 9 &&
                this.tokenStream.get(2).getTokenValue().equals("and") &&
                this.tokenStream.get(4).getTokenValue().equals("on") &&
                this.tokenStream.get(6).getTokenValue().equals("and"))) {
            throw new malformedQueryException();
        }
        if (!isValidIdentifier(this.tokenStream.get(1))) {
            throw new invalidIdentifierException(this.tokenStream.get(1).getTokenValue());
        }
        if (!isValidIdentifier(this.tokenStream.get(3))) {
            throw new invalidIdentifierException(this.tokenStream.get(3).getTokenValue());
        }
        if (!isValidIdentifier(this.tokenStream.get(5))) {
            throw new invalidIdentifierException(this.tokenStream.get(5).getTokenValue());
        }
        if (!isValidIdentifier(this.tokenStream.get(7))) {
            throw new invalidIdentifierException(this.tokenStream.get(7).getTokenValue());
        }

        // Get table names
        String tableName1 = this.tokenStream.get(1).getTokenValue();
        String tableName2 = this.tokenStream.get(3).getTokenValue();

        // Get attribute names
        String attribute1 = this.tokenStream.get(5).getTokenValue();
        String attribute2 = this.tokenStream.get(7).getTokenValue();

        return new JoinCMD(tableName1, tableName2, attribute1, attribute2);
    }
}
