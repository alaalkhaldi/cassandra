package org.apache.cassandra.metadata;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.MessagingService;

/**
 * A Runnable that aborts if it doesn't start running before it times out
 */
public abstract class DroppableRunnable implements Runnable
{
    private final long constructionTime = System.currentTimeMillis();
    private final MessagingService.Verb verb;

    public DroppableRunnable(MessagingService.Verb verb)
    {
        this.verb = verb;
    }

    public final void run()
    {
        if (System.currentTimeMillis() > constructionTime + DatabaseDescriptor.getTimeout(verb))
        {
            MessagingService.instance().incrementDroppedMessages(verb);
            return;
        }

        try
        {
            runMayThrow();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    abstract protected void runMayThrow() throws Exception;
}