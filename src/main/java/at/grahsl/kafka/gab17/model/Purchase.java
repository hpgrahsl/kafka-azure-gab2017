package at.grahsl.kafka.gab17.model;

public class Purchase {

    public final String buyingLocation;
    public final String productCategory;
    public final Double orderTotal;
    public final String paymentType;

    public Purchase(String buyingLocation, String productCategory, Double orderTotal, String paymentType) {
        this.buyingLocation = buyingLocation;
        this.productCategory = productCategory;
        this.orderTotal = orderTotal;
        this.paymentType = paymentType;
    }

    @Override
    public String toString() {
        return "Purchase{" +
                "buyingLocation='" + buyingLocation + '\'' +
                ", productCategory='" + productCategory + '\'' +
                ", orderTotal=" + orderTotal +
                ", paymentType='" + paymentType + '\'' +
                '}';
    }
}
