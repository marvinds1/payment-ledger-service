package io.github.marvinds1.ledger.transfer;

import io.github.marvinds1.ledger.common.BusinessRuleException;
import io.github.marvinds1.ledger.common.NotFoundException;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.function.Supplier;
import org.springframework.stereotype.Component;

@Component
class TransferMetrics {

    static final String TRANSFERS = "ledger.transfers";
    static final String DURATION = "ledger.transfer.duration";
    static final String VOLUME = "ledger.transfer.volume";

    private final MeterRegistry registry;

    TransferMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    TransferResult measure(String operation, Supplier<TransferResult> action) {
        Timer.Sample sample = Timer.start(registry);
        TransferResult result;
        try {
            result = action.get();
        } catch (RuntimeException e) {
            record(sample, operation, "failed", failureReason(e));
            throw e;
        }
        Transfer transfer = result.transfer();
        if (result.replayed()) {
            record(sample, operation, "replayed", reasonOrNone(transfer));
        } else if (transfer.status() == TransferStatus.COMPLETED) {
            record(sample, operation, "completed", "none");
            Counter.builder(VOLUME)
                    .description("Money moved by completed transfers, in minor units")
                    .tags("operation", operation, "currency", transfer.currency())
                    .register(registry)
                    .increment(transfer.amount());
        } else {
            record(sample, operation, "rejected", reasonOrNone(transfer));
        }
        return result;
    }

    private void record(Timer.Sample sample, String operation, String outcome, String reason) {
        sample.stop(Timer.builder(DURATION)
                .description("Time to process a transfer request, including idempotent replays")
                .tags("operation", operation, "outcome", outcome)
                .register(registry));
        Counter.builder(TRANSFERS)
                .description("Transfer requests by outcome")
                .tags("operation", operation, "outcome", outcome, "reason", reason)
                .register(registry)
                .increment();
    }

    private static String reasonOrNone(Transfer transfer) {
        return transfer.failureReason() == null ? "none" : transfer.failureReason();
    }

    private static String failureReason(RuntimeException e) {
        if (e instanceof BusinessRuleException rule) {
            return rule.code();
        }
        if (e instanceof NotFoundException) {
            return "NOT_FOUND";
        }
        return "UNEXPECTED";
    }
}
