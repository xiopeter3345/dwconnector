package com.microsoft.synapse.utils

case class DWConnectorException(message:String, throwable:Throwable) extends Exception(message, throwable) {

}

object DWConnectorException {
  def apply(message:String): DWConnectorException = {
    DWConnectorException(message, null)
  }
}
