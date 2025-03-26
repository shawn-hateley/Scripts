function getZabbixData() {
  // Make a POST request with a JSON payload.
const data = {
    jsonrpc : "2.0",
    method : "item.get",
    params : {"output": ["itemid","name","status"],
              "hostids": "10557",
              "search": {
                         "key_": "Honeywell"
                     },
            },
    id : 5,
    auth:'f6efb0c67da162717b08072cd02881683a270b5c8a8cd3d361edde5515e74b55', //Quadra Zabbix
  };
  const options = {
    method: 'post',
    contentType: 'application/json-rpc',
    // Convert the JavaScript object to a JSON string.
    payload: JSON.stringify(data),
  };
  //Logger.log(options);
  const response = UrlFetchApp.fetch('http://207.102.156.134:12050/zabbix/api_jsonrpc.php', options); //Quadra Zabbix

  //Logger.log(response.getContentText());
}