function TanksActive() {
  var id = "1UWmhuW6yQ31vK5e0ig6WxPCwU7LdX7l4fsspHwXY-Dw"
  var tankSheet = SpreadsheetApp.openById(id).getSheetByName('Tanks in use');

  var rows = tankSheet.getDataRange();
  var numRows = rows.getNumRows();
  var values = rows.getValues();

  console.log(values);

getActiveTanks();