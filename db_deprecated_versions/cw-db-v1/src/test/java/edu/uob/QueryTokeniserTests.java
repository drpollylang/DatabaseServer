package edu.uob;

import java.util.*;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


public class QueryTokeniserTests {
    private QueryTokeniser tokeniser;

    // Create a new server _before_ every @Test
    @BeforeEach
    public void setup() {
        tokeniser = new QueryTokeniser();
    }

    @Test
    public void tokeniseQueryCreateDB() {
        ArrayList<Token> tokens = tokeniser.getTokens("USE testdb;");
        //for (int i = 0; i < tokens.size(); i++) System.out.println(tokens.get(i));
        assertEquals("use", tokens.get(0).getTokenValue(), "First outputted token incorrect. Expected: 'USE'. Actual: " + tokens.get(0));
    }

    // TODO: change assertTrue to assertEquals and correct to account for Token
    @Test
    public void tokeniseQuerySelect() {
        ArrayList<Token> tokens = tokeniser.getTokens("SELECT id, Name, Age FROM people WHERE (Age < 28);");
        for (int i = 0; i < tokens.size(); i++) System.out.println(tokens.get(i).getTokenValue());
        assertEquals("select", tokens.get(0).getTokenValue(), "First outputted token incorrect. Expected: 'SELECT'. Actual: " + tokens.get(0));
        /*assertTrue("'id'".equals(tokens.get(1)), "First outputted token incorrect. Expected: ''id''. Actual: " + tokens.get(1));
        assertTrue(",".equals(tokens.get(2)), "First outputted token incorrect. Expected: ','. Actual: " + tokens.get(2));
        assertTrue("'Name'".equals(tokens.get(3)), "First outputted token incorrect. Expected: 'Name'. Actual: " + tokens.get(3));
        assertTrue(",".equals(tokens.get(4)), "First outputted token incorrect. Expected: ','. Actual: " + tokens.get(4));
        assertTrue("'Age'".equals(tokens.get(5)), "First outputted token incorrect. Expected: 'Age'. Actual: " + tokens.get(5));
        assertTrue("FROM".equals(tokens.get(6)), "First outputted token incorrect. Expected: 'FROM'. Actual: " + tokens.get(6));
        assertTrue("people".equals(tokens.get(7)), "First outputted token incorrect. Expected: 'people'. Actual: " + tokens.get(7));
        assertTrue("WHERE".equals(tokens.get(8)), "First outputted token incorrect. Expected: 'WHERE'. Actual: " + tokens.get(8));
        assertTrue("(".equals(tokens.get(9)), "First outputted token incorrect. Expected: '('. Actual: " + tokens.get(9));
        assertTrue("Age".equals(tokens.get(10)), "First outputted token incorrect. Expected: 'Age'. Actual: " + tokens.get(10));
        assertTrue("<".equals(tokens.get(11)), "First outputted token incorrect. Expected: '<'. Actual: " + tokens.get(11));
        assertTrue("28".equals(tokens.get(12)), "First outputted token incorrect. Expected: '28'. Actual: " + tokens.get(12));
        assertTrue(")".equals(tokens.get(13)), "First outputted token incorrect. Expected: ')'. Actual: " + tokens.get(13));
        assertTrue(";".equals(tokens.get(14)), "First outputted token incorrect. Expected: ';'. Actual: " + tokens.get(14));
    */
    }


}