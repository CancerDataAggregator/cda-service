package bio.terra.cda.app.service;

import java.text.CharacterIterator;
import java.text.StringCharacterIterator;

public class FilterUtils{
    public static String trimExtraneousParentheses(String query) {
        if(query.startsWith("(") && query.endsWith(")")){
            //Determine if the opening and closing parens match with each other...
            CharacterIterator it = new StringCharacterIterator(query);
            it.next();
            int count = 1;
            while (it.current() != CharacterIterator.DONE) {
                if(it.current() == '(')
                    count++;
                if(it.current() == ')') {
                    count--;
                    //this case occurs when the opening paren has been matched before we
                    //get to the end. E.g.: "((a =4)) OR (b=10)"
                    if(count == 0 && (it.getIndex() < (query.length()-1)))
                        return query;
                }
                it.next();
            }
            //This case means that the opening paren matches the closing paren,
            //E.g.: "(((a=4) OR (b=10)))". We recurse to continue stripping off
            //these extraneous parens
            if(count == 0)
                return trimExtraneousParentheses(query.substring(1, query.length()-1));
        }
        //If we don't have opening and closing parens, there isn't anything to trim
        return query;
    }

    public static String parenthesisSubString(String startingString) { // Helper function to extract the string between the first
        // parenthesis and it's closing one
        int openParenthesisCount = 1;
        int indexCursor = 0;
        while (openParenthesisCount > 0 && (indexCursor+1) < startingString.length()) {
            indexCursor += 1;
            if (startingString.charAt(indexCursor) == '(') {
                openParenthesisCount += 1;
            } else if (startingString.charAt(indexCursor) == ')') {
                openParenthesisCount -= 1;
            }
        }
        return startingString.substring(0, indexCursor+1);
    }
}