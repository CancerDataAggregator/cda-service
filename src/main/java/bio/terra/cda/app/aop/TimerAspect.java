package bio.terra.cda.app.aop;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.Signature;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Aspect
@Component
public class TimerAspect {

    private static final Logger logger = LoggerFactory.getLogger(TimerAspect.class);

    @Around("@annotation(bio.terra.cda.app.aop.TrackExecutionTime)")
    public Object executionTimer(ProceedingJoinPoint joinPoint) throws Throwable {
        long start = System.currentTimeMillis();
        Object proceed = joinPoint.proceed();
        long executionTime = System.currentTimeMillis() - start;
        Signature signature = joinPoint.getSignature();
        logger.info("--Execution Timer: {} executed in {}ms", signature, executionTime);
        return proceed;
    }
}
