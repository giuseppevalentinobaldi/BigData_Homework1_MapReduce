package top5;

public class Average implements Comparable<Average> {
	private int sumTotal;
	private int countTotal;

	public Average(int sumTotal) {
		this.sumTotal = sumTotal;
		this.countTotal = 1;
	}

	public int getCountTotal() {
		return countTotal;
	}

	public void setCountTotal(int countTotal) {
		this.countTotal = countTotal;
	}

	public int getSumTotal() {
		return sumTotal;
	}

	public void setSumTotal(int sumTotal) {
		this.sumTotal = sumTotal;
	}

	public void updateAverage(int value) {
		this.setSumTotal(value + this.getSumTotal());
		this.setCountTotal(this.getCountTotal() + 1);
	}

	public float getAverage() {

		return (float)this.getSumTotal()/(float)this.getCountTotal();

	}

	public int compareTo(Average avg) {
		if (this.getAverage() > avg.getAverage()) {
			return 1;
		}
		if (this.getAverage() < avg.getAverage()) {
			return -1;
		}
		return 0;
	}

}
