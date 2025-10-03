package edu.uob;

import java.util.List;
import java.util.Arrays;

public class Token {
    private TokenType tokenType;
    private String tokenValue;
    final static List<String> keywords = Arrays.asList("use", "create", "drop", "alter", "insert",
            "select", "update", "delete", "join", "database", "table", "into", "values", "from",
            "where", "set", "on");
    final static List<String> alterationTypes = Arrays.asList("add", "drop");
    final static List<String> symbols = Arrays.asList("!", "#", "$", "%", "&", "(", ")", "*", "+", ",", "-", ".",
            "/", ":", ";", ">", "=", "<", "?", "@", "[", "\\", "]", "^", "_", "`", "{", "}", "~");
    final static List<String> booleanOperators = Arrays.asList("and", "or");
    final static List<String> comparators = Arrays.asList("==", ">", "<", ">=", "<=", "!=", "like");
    final static List<String> booleanLiterals = Arrays.asList("true", "false");

    public Token(String token) {
        this.tokenType = this.findTokenType(token);
        this.tokenValue = this.findTokenValue(token);
    }


    private TokenType findTokenType(String token) {
        // Make queries case-insensitive by making tokens lowercase
        token = token.toLowerCase();

        // Find the token type - string matching
        if (keywords.contains(token)) return TokenType.keyword;
        else if (alterationTypes.contains(token)) return TokenType.alterationType;
        else if (symbols.contains(token)) return TokenType.symbol;
        else if (booleanOperators.contains(token)) return TokenType.booleanOperator;
        else if (comparators.contains(token)) return TokenType.comparator;
        else if (isFloat(token)) return TokenType.floatLiteral;
        else if (isDigitSequence(token)) return TokenType.integerLiteral;
        else if (booleanLiterals.contains(token)) return TokenType.booleanLiteral;
        else if (isCharLiteral(token)) return TokenType.charLiteral;
        else if (isStringLiteral(token)) return TokenType.stringLiteral;
        else if (token.equals("null")) return TokenType.Null;
        else return TokenType.plainText;
    }


    private String findTokenValue(String token) {
        // If '', remove quotes
        if (token.contains("'")) {
            return token.replaceAll("'", "");
        }
        else {
            // Convert token to lowercase to ensure query case-insensitivity
            return token;
        }
    }


    private static boolean isDigitSequence(String token) {
        for (int i = 0; i < token.length(); i++) {
            if (!Character.isDigit(token.charAt(i))) return false;
        }
        return true;
    }


    private static boolean isFloat(String token) {
        if (!token.contains(".")) return false;

        if (token.contains("+") || token.contains("-")) {
            if (!(token.charAt(0) == '+' || token.charAt(0) == '-')) return false;
            token = token.replace("+|-", ""); // remove + or -
        }

        // Length of array of DigitSequences should be 2 - no more than 1 . in float
        String[] splitToken = token.split("\\.");
        if (splitToken.length != 2) return false;
        return isDigitSequence(splitToken[0]) && isDigitSequence(splitToken[1]);
    }


    private static boolean isCharLiteral(String token) {
        // [CharLiteral] ::= [Space] | [Letter] | {Symbol] | [Digit]
        if (token.length() > 1) return false;
        return Character.isAlphabetic(token.charAt(0)) ||
                Character.isSpaceChar(token.charAt(0));
    }


    private static boolean isStringLiteral(String token) {
        // "" | [CharLiteral] | [StringLiteral] [CharLiteral]
        if (token.isEmpty()) return true;
        if (isCharLiteral(token)) return true;
        if (token.length() > 1) return isStringLiteral(token.substring(0, token.length()-1));
        return false;
    }


    // Getters for private attributes
    public TokenType getTokenType() {
        return this.tokenType;
    }
    public String getTokenValue() {
        return this.tokenValue;
    }

    public void setTokenToLowerCase() {
        this.tokenValue = this.tokenValue.toLowerCase();
    }

    public enum TokenType {
        keyword,
        alterationType,
        symbol,
        booleanOperator,
        comparator,
        integerLiteral,
        floatLiteral,
        booleanLiteral,
        charLiteral,
        stringLiteral,
        Null,
        plainText
    }


    public enum TargetType {
        DATABASE,
        TABLE
    }


    public enum AlterationType {
        ADD,
        DROP
    }

    public enum BooleanOperator {
        AND,
        OR
    }
}
