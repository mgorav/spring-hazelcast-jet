package com.gonnect.hazelcast.jet.peipeline;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.Objects;

/**
 * Information about a specific trade event that took place on a
 * financial exchange or any other kind of market.
 * <p>
 * A "trade event" is defined as the action by which one market
 * participant sells a certain amount (the "quantity") of a financial
 * instrument (the "ticker", most often a stock specified as the
 * short name of the company), to another market participant, at a
 * certain price.
 */
public class Trade implements Serializable {

    private final long time;
    private final String ticker;
    private final long quantity;
    private final long price;

    Trade(long time, @Nonnull String ticker, long quantity, long price) {
        this.time = time;
        this.ticker = Objects.requireNonNull(ticker);
        this.quantity = quantity;
        this.price = price;
    }

    /**
     * Event time of the trade.
     */
    public long getTime() {
        return time;
    }

    /**
     * Name of the instrument being traded.
     */
    @Nonnull
    public String getTicker() {
        return ticker;
    }

    /**
     * Quantity of the trade, the amount of the instrument that has been
     * traded.
     */
    public long getQuantity() {
        return quantity;
    }

    /**
     * Price at which the transaction took place.
     */
    public long getPrice() {
        return price;
    }

    @Override
    public String toString() {
        return "Trade{time=" + time + ", ticker='" + ticker + '\'' + ", quantity=" + quantity
                + ", price=" + price + '}';
    }
}
