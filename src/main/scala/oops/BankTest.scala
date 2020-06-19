package oops

object BankTest {

  def main(args: Array[String]): Unit = {
    var balance = 1000
    var acc = new BankAccount(balance)

    acc.deposit(100)
    acc.withdraw(150)
    println(acc)

    var check = new CheckingAccount(balance, 1)
    check.deposit(1)
    check.withdraw(1)
    println(check)
  }
}

class BankAccount(initialBalance: Double) {
  private var _balance = initialBalance

  def deposit(amount: Double) = {
    _balance += amount
    _balance
  }

  def withdraw(amount: Double) = {
    _balance -= amount
    _balance
  }

  def balance() = _balance

  override def toString: String = s"Account{balance = ${_balance}}"
}

class CheckingAccount(initialBalance: Double, val charges: Double) extends BankAccount(initialBalance) {

  override def deposit(amount: Double): Double = {
    super.deposit(amount - charges)
  }

  override def withdraw(amount: Double): Double = {
    super.withdraw(amount + charges)
  }
}