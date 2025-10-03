package edu.uob;

import java.util.*;

import edu.uob.DBException.*;
import edu.uob.Token.*;

public class QueryParser {
    private ArrayList<Token> tokenStream;

    public QueryParser(ArrayList<Token> tokenStream) {
        this.tokenStream = tokenStream;
    }

    public DBcmd parse() throws DBException {
        // Pass query to method depending on commandType
        if (this.tokenStream.get(0).getTokenType() != TokenType.keyword) {
            throw new invalidKeywordException(this.tokenStream.get(0).getTokenValue());
        }
        this.convertTokenStreamToLower();

        String commandType = this.tokenStream.get(0).getTokenValue();
        return switch (commandType) {
            case "use" -> new UseCMD(this.tokenStream);
            case "create" -> new CreateCMD(this.tokenStream);
            case "drop" -> new DropCMD(this.tokenStream);
            case "alter" -> new AlterCMD(this.tokenStream);
            case "insert" -> new InsertCMD(this.tokenStream);
            case "select" -> new SelectCMD(this.tokenStream);
            case "update" -> new UpdateCMD(this.tokenStream);
            case "delete" -> new DeleteCMD(this.tokenStream);
            case "join" -> new JoinCMD(this.tokenStream);
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
}