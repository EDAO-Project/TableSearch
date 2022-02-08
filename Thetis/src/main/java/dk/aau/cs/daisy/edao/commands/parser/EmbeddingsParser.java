package dk.aau.cs.daisy.edao.commands.parser;

import java.io.*;
import java.text.ParseException;
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
    private EmbeddingToken prev = null;

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

    // Note this method can throw a ParsingException even if the file has been parsed correctly.
    // This is when the file ends with a newline character, since the next read will return -1,
    // and hence the lexeme is empty and cannot be parsed.
    // In this case, null is returned.
    @Override
    public EmbeddingToken next()
    {
        if (this.isClosed)
            return null;

        try
        {
            StringBuilder lexemeBuilder = new StringBuilder();
            boolean readAnything = false;
            int c;

            while ((c = this.input.read()) != -1)
            {
                if (!readAnything && (Character.isLetter(c) || Character.isDigit(c) || c == '-' || c == '.' || c == '/' || c == ':'))
                    readAnything = true;

                else if (readAnything && lexemeBuilder.length() > 0 && (c == ' ' || c == '\n') &&
                        (parseDecimal(lexemeBuilder.toString()) || lexemeBuilder.toString().contains("://")))
                    break;

                else if (!readAnything && (c == ' ' || c == '\n'))
                    continue;

                lexemeBuilder.append((char) c);
            }

            if (c == -1)
                this.isClosed = true;

            String lexeme = lexemeBuilder.toString();

            if (lexeme.contains("://"))
            {
                this.prev = new EmbeddingToken(lexeme, EmbeddingToken.Token.ENTITY);
                return this.prev;
            }

            else if (parseDecimal(lexeme))
            {
                this.prev = new EmbeddingToken(lexeme, EmbeddingToken.Token.VALUE);
                return this.prev;
            }

            else if (lexeme.isBlank())
                return null;

            else
                throw new ParsingException("Could not parse lexeme '" + lexeme + "'");
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
    public EmbeddingToken prev()
    {
        return this.prev;
    }
}
