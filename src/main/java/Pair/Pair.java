package Pair;

public class Pair<FirstType,SecondType> {
    FirstType first;
    SecondType second;
    public Pair(FirstType first,SecondType second){
        this.first=first;
        this.second=second;
    }
    public FirstType getLeft(){
        return first;
    }
    public SecondType getRight() {
        return second;
    }

    public final FirstType getKey(){
        return this.getLeft();
    }
    public SecondType getValue(){
        return this.getRight();
    }

    public String toString() {
        return "(" + this.getLeft() + ',' + this.getRight() + ')';
    }

    public String toString(String format) {
        return String.format(format, this.getLeft(), this.getRight());
    }
}
