package com.aws.kinesis.record.handler;

import com.sun.istack.internal.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HandlerFactory {
  private static Logger logger = LoggerFactory.getLogger(HandlerFactory.class);

  private HandlerFactory() {}

  public static HandlerFactory getInstance() {
    return LazyHolder.INSTANCE;
  }

  private static class LazyHolder {
    private static final IRecordsHandler StdoutHandler = new StdoutHandler();
    private static final IRecordsHandler DebugoutHandler = new DebugoutHandler();
    private static final HandlerFactory INSTANCE = new HandlerFactory();
  }

  @Nullable
  public IRecordsHandler getHandler(HandlerType handlerType, String...handlerArgs){
    switch (handlerType) {
      case StdoutHandler:
        return LazyHolder.StdoutHandler;
      case DebugoutHandler:
        return LazyHolder.DebugoutHandler;
      case TmpFileoutHandler:
        if (handlerArgs.length >= 1) {
          return new TmpFileoutHandler(handlerArgs[0]);
        } else return null;
      default:
        return null;
    }
  }

  public IRecordsHandler[] mergeHandler(IRecordsHandler handler, IRecordsHandler...handlers) {
    final IRecordsHandler[] totalHandlers;

    if (handlers.length > 0) {
      logger.debug("handler count is more than 1, merge handler list.");
      totalHandlers = new IRecordsHandler[handlers.length + 1];
      totalHandlers[0] = handler;

      System.arraycopy(handlers, 0, totalHandlers, 1, handlers.length);
    } else {
      logger.debug("handler count is 1.");

      totalHandlers = new IRecordsHandler[1];
      totalHandlers[0] = handler;
    }
    logger.debug("total handler count is " + totalHandlers.length);

    return totalHandlers;
  }
}
