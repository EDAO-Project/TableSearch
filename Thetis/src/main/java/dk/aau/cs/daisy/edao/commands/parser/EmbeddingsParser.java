package dk.aau.cs.daisy.edao.commands.parser;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
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

    private InputStream input;
    private boolean isClosed = false;
    private int lexCount = 0;

    public EmbeddingsParser(String content)
    {
        this.input = new ByteArrayInputStream(content.getBytes());
    }

    public EmbeddingsParser(InputStream inputStream)
    {
        this.input = inputStream;
    }

    // Lex by space
    private static List<String> lex(String content)
    {
        return List.of(content.split(" "));
    }

    @Override
    public String nextLexeme()
    {
        return next().getLexeme();
    }

    @Override
    public boolean hasNext()
    {
        return !this.isClosed;
    }

    @Override
    public EmbeddingToken next()
    {
        if (this.isClosed)
            return null;

        try
        {
            StringBuilder lexemeBuilder = new StringBuilder();
            int c;

            while ((c = this.input.read()) != -1)
            {
                if (c == ' ' || c == '\n')
                    break;

                lexemeBuilder.append((char) c);
            }

            if (c != ' ')
                this.isClosed = true;

            this.lexCount++;
            String lexeme = lexemeBuilder.toString();

            if (lexeme.startsWith("http"))
                return new EmbeddingToken(lexeme, EmbeddingToken.Token.ENTITY);

            else if (parseDecimal(lexeme))
                return new EmbeddingToken(lexeme, EmbeddingToken.Token.VALUE);

            else
                throw new RuntimeException("Could not parse lexeme '" + lexeme + "'");
        }

        catch (IOException exception)
        {
            return null;
        }
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
        if (this.lexCount - count < 0)
            throw new IllegalArgumentException("Reversed too far");

        try
        {
            this.input.reset();

            for (int i = 0; i < this.lexCount - count; i++)
            {
                next();
            }
        }

        catch (IOException exc) {}
    }
}
