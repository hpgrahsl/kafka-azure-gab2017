package at.grahsl.kafka.gab17.model;

public class PurchaseStats {

    int counter;
    double sumTotal;
    double minTotal = Double.MAX_VALUE;
    double maxTotal = Double.MIN_VALUE;
    double avgTotal;

    public PurchaseStats accumulate(Purchase purchase) {

        this.counter++;
        this.sumTotal += purchase.orderTotal;
        this.minTotal = purchase.orderTotal < minTotal
                            ? purchase.orderTotal : minTotal;
        this.maxTotal = purchase.orderTotal > maxTotal
                            ? purchase.orderTotal : maxTotal;

        return this;
    }

    public PurchaseStats calcAvgTotal() {
        this.avgTotal = this.sumTotal / this.counter;
        return this;
    }

    @Override
    public String toString() {
        return "PurchaseStats{" +
                "counter=" + counter +
                ", sumTotal=" + sumTotal +
                ", minTotal=" + minTotal +
                ", maxTotal=" + maxTotal +
                ", avgTotal=" + avgTotal +
                '}';
    }
}
