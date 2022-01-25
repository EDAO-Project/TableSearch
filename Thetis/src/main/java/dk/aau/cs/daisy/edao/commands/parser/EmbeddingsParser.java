package dk.aau.cs.daisy.edao.commands.parser;

import java.util.List;

public class EmbeddingsParser implements Parser<EmbeddingsParser.EmbeddingToken>
{
    public static final class EmbeddingToken
    {
        String lexeme;
        Token t;

        public enum Token {ENTITY, VALUE}

        public EmbeddingToken(String lexeme, Token t)
        {
            this.lexeme = lexeme;
            this.t = t;
        }

        public String getLexeme()
        {
            return this.lexeme;
        }

        public Token getToken()
        {
            return this.t;
        }
    }

    private List<String> content;
    private int pointer = 0;

    public EmbeddingsParser(String content)
    {
        this.content = lex(content);
    }

    // Lex by space
    private static List<String> lex(String content)
    {
        return List.of(content.split(" "));
    }

    // Does not move iterator pointer
    // Returns null if the points already points to the last lexeme
    @Override
    public String nextLexeme()
    {
        if (this.pointer == this.content.size() - 1)
            return null;

        return this.content.get(this.pointer + 1);
    }

    @Override
    public boolean hasNext()
    {
        return this.pointer < this.content.size();
    }

    @Override
    public EmbeddingToken next()
    {
        if (this.pointer == this.content.size())
            return null;

        String lexeme = this.content.get(this.pointer++).replaceAll("\n", "");

        if (lexeme.startsWith("http"))
            return new EmbeddingToken(lexeme, EmbeddingToken.Token.ENTITY);

        else if (parseDecimal(lexeme))
            return new EmbeddingToken(lexeme, EmbeddingToken.Token.VALUE);

        else
            throw new RuntimeException("Could not parse lexeme '" + lexeme + "'");
    }

    private static boolean parseDecimal(String lexeme)
    {
        try
        {
            Double.parseDouble(lexeme);
            return true;
        }

        catch (NumberFormatException exc)
        {
            return false;
        }
    }

    @Override
    public void reverse(int count)
    {
        if (this.pointer - count < 0)
            throw new IllegalArgumentException("Reversed too far");

        this.pointer -= count;
    }
}
