package exceptions;

public class DeadLetterException extends Exception {
  public DeadLetterException(String message){
    super(message);
  }

}
