package scheduling.dynamicScheduler.dynamic;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.Trigger;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.stereotype.Component;
import scheduling.dynamicScheduler.service.ScheduleTask;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.sql.SQLException;
import java.time.LocalDateTime;

@Component
public class DynamicChangeScheduler {
    private ThreadPoolTaskScheduler scheduler;
    private String cron = "*/5 * * * * *";

    @Autowired
    private ScheduleTask scheduleTask;

    public void startScheduler() {
        scheduler = new ThreadPoolTaskScheduler();
        scheduler.initialize();
        // scheduler setting
        scheduler.schedule(getRunnable(), getTrigger());
    }

    public void changeCronSet(String cron) {
        this.cron = cron;
    }

    public void stopScheduler() {
        scheduler.shutdown();
    }

    private Runnable getRunnable() {
        // do something
        return () -> {
            try {
                scheduleTask.task1();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            System.out.println(LocalDateTime.now().toString());
        };
    }

    private Trigger getTrigger() {
        // cronSetting
        return new CronTrigger(cron);
    }

    @PostConstruct
    public void init() {
        startScheduler();
    }

    @PreDestroy
    public void destroy() {
        stopScheduler();
    }
}