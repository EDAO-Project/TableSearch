package dk.aau.cs.daisy.edao.system;

public class Logger
{
    public enum Level
    {
        INFO(1), DEBUG(2), ERROR(3);

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
                    return INFO.name();

                case 2:
                    return ERROR.name();

                case 3:
                    return DEBUG.name();

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

            return null;
        }
    }

    private static int prevLength = 0;

    public static void log(Level level, String message)
    {
        Level configuredLevel = Level.parse(Configuration.getLogLevel());

        if (configuredLevel != null && level.getLevel() >= configuredLevel.getLevel())
        {
            clearChannel();
            System.out.print(message + "\r");
            prevLength = message.length();
        }
    }

    public static void logNewLine(Level level, String message)
    {
        Level configuredLevel = Level.parse(Configuration.getLogLevel());

        if (configuredLevel != null && level.getLevel() >= configuredLevel.getLevel())
            System.out.println("\n" + message);
    }

    private static void clearChannel()
    {
        for (int i = 0; i < prevLength; i++)
        {
            System.out.print(" ");
        }
    }
}
