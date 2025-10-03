package edu.uob;

import java.util.Arrays;
import java.util.ArrayList;

public class QueryTokeniser {
    static String[] specialCharacters = {"(",")",",",";"};
	static String terminalChar = ";";
	static int minimumQueryLength = 3;

    public QueryTokeniser() {
    }

	private String[] tokenise(String input) {
		// Add in some extra padding spaces either side of the "special characters"...
  		// so we can be SURE that they are separated by AT LEAST one space (possibly more)
  		for(int i=0; i < specialCharacters.length; i++) {
    		input = input.replace(specialCharacters[i], " " + specialCharacters[i] + " ");
  		}
  		// Remove any double spaces (the previous padding activity might have introduced some of these)
  		while (input.contains("  ")) input = input.replace("  ", " "); // Replace two spaces by one
  		// Remove any whitespace from the beginning and the end that might have been introduced
  		input = input.trim();
  		// Finally split on the space char (since there will now ALWAYS be a SINGLE space between tokens)
  		return input.split(" ");
	}


	// TODO: modularise this method! Too long/complex
    private ArrayList<Token> tokeniseQuery(String query) throws Exception {
        // Split the query on single quotes (to separate out query text from string literals)
        String[] fragments = query.split("'");
		ArrayList<String> tokens = new ArrayList<String>();
		for (int i=0; i<fragments.length; i++) {
			// Every other fragment is a string literal, so just add it straight to "result" token list
    		if (i%2 != 0) tokens.add("'" + fragments[i] + "'");
    		// If it's not a string literal, it must be query text (which needs further processing)
    		else {
      			// Tokenise the fragment into an array of strings - this is the "clever" bit !
      			String[] nextBatchOfTokens = tokenise(fragments[i]);
      			// Then copy all the tokens into the "result" list (needs a bit of conversion)
      			tokens.addAll(Arrays.asList(nextBatchOfTokens));
    		}
  		}
		// Error handling: according to the BNF, shortest possible query contains 2 tokens. Check that 
		// this.tokens is at least 2 tokens long.
		// Also check that the last token is a semi-colon (terminating character)
		// TODO: define own error type for this (like in OXO)?
		if (tokens.size() < minimumQueryLength) {
			throw new Exception("Valid queries must contain at least " + minimumQueryLength + " words! Your query only contains " + tokens.size() + " words. Please input a valid query.");
		}
		if (!tokens.get(tokens.size()-1).equals(terminalChar)) {
			throw new Exception("Valid queries must end with a semicolon (;). Please input a valid query.");
		}
  		// Finally, loop through the result array list, convert each token String to the Token type
		ArrayList<Token> outputTokens = new ArrayList<Token>();
  		for (int i = 0; i < tokens.size(); i++) {
			  Token newToken = new Token(tokens.get(i));
			  outputTokens.add(i, newToken);
		}
		  return outputTokens;
    }

	public ArrayList<Token> getTokens(String query) {
		try {
			return tokeniseQuery(query);
		} catch (Exception error) {
			System.out.println("Error: " + error);
			return null;
		}
	}

}
