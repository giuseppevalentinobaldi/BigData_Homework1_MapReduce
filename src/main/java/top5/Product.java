package top5;

public class Product implements Comparable<Product> {
	private String productId;
	private float average;

	public Product(String productId, float average) {
		this.setProductId(productId);
		this.setAverage(average);
	}

	public String getProductId() {
		return productId;
	}

	public void setProductId(String productId) {
		this.productId = productId;
	}

	public float getAverage() {
		return average;
	}

	public void setAverage(float average) {
		this.average = average;
	}

	public String toString() {
		return this.getProductId() + "\t" + this.getAverage();
	}

	
	public int compareTo(Product p) {
		if (this.getAverage() > p.getAverage()) {
			return 1;
		}
		if (this.getAverage() < p.getAverage()) {
			return -1;
		}
		return 0;
	}

}
