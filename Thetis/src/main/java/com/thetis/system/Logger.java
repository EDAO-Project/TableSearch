package com.thetis.system;

import java.io.PrintStream;
import java.util.Date;

/**
 * Class for logging at different logging levels
 */
public class Logger
{
    public enum Level
    {
        DEBUG(1), INFO(2), RESULT(3), ERROR(4);

        private int level;

        Level(int level)
        {
            this.level = level;
        }

        public String toString()
        {
            switch (this.level)
            {
                case 1:
                    return DEBUG.name();

                case 2:
                    return INFO.name();

                case 3:
                    return RESULT.name();

                case 4:
                    return ERROR.name();

                default:
                    return null;
            }
        }

        public int getLevel()
        {
            return this.level;
        }

        public static Level parse(String str)
        {
            if ("info".equals(str.toLowerCase()))
                return INFO;

            else if ("error".equals(str.toLowerCase()))
                return ERROR;

            else if ("debug".equals(str.toLowerCase()))
                return DEBUG;

            else if ("result".equals(str.toLowerCase()))
                return RESULT;

            return null;
        }
    }

    private static int prevLength = 0;
    private static boolean prevWasNewLine = false;
    private static PrintStream stream = System.out;

    public static void setPrintStream(PrintStream printStream)
    {
        stream = printStream;
    }

    public static void log(Level level, String message)
    {
        Level configuredLevel = Level.parse(Configuration.getLogLevel());

        if (configuredLevel != null && level.getLevel() >= configuredLevel.getLevel())
        {
            if (prevWasNewLine)
                stream.println();

            else
                clearChannel();

            String msg = "(" + new Date() + ") - " + level + ": " + message + "\r";
            stream.print(msg);
            prevLength = msg.length();
            prevWasNewLine = false;
        }
    }

    public static void logNewLine(Level level, String message)
    {
        Level configuredLevel = Level.parse(Configuration.getLogLevel());

        if (configuredLevel != null && level.getLevel() >= configuredLevel.getLevel())
        {
            stream.print("\n(" + new Date() + ") - " + level + ": " + message);
            prevWasNewLine = true;
        }
    }

    private static void clearChannel()
    {
        for (int i = 0; i < prevLength; i++)
        {
            stream.print(" ");
        }

        stream.print("\r");
    }
}
