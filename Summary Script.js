function Summary() {
  var sheets = SpreadsheetApp.getActiveSpreadsheet().getSheets();
  var idSheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName('Keys');
  var ids = idSheet.getRange(1,2,idSheet.getLastRow(),1).getValues();
    
  for (var t = 0; t < ids.length; t++){ // Do this for each spreadsheet key
    var id = ids[t];
    if (id == "") continue; //check to see if the cell is blank
    var dataSheets = SpreadsheetApp.openById(id).getSheets();

    for (var a = 1; a < sheets.length; a++){ // Do this for each sheet on the summary spreadsheet
  
    var summarySheet = sheets[a];
    var summaryHeader = summarySheet.getRange(1,1,1,summarySheet.getLastColumn()).getValues()[0];
    var summaryHeader2 = summarySheet.getRange(2,1,1,summarySheet.getLastColumn()).getValues()[0];
    var j = 3; // start setting results in row 4 
    var filter = "";
    
    summarySheet.getRange(j,1,summarySheet.getLastRow(),summarySheet.getLastColumn()).clear();
   
    var filterColor = summarySheet.getRange(1,1,1,summarySheet.getLastColumn()).getBackgrounds()[0]; //Find the cell with the orange background and set it as the filter

    
    for (var f = 0; f < filterColor.length; f++){
      if (filterColor[f] == "#ff9900"){
           filter = summaryHeader[f]        
      }
     }
 
     if (filter == ""){
       filter = summaryHeader[0]; // If no cell with an orange background is found filter on the first column
      }
    

      
    // Get the column numbers for each variable
    for(var s = 0; s < dataSheets.length; s++){ //Do this for each sheet 
    
      var processed = []; 
      var map = [];
      var sheetHeader = dataSheets[s].getRange(1,1,1,dataSheets[s].getLastColumn()).getValues()[0]; //Get the header for the sheet      
  
      
      
      //Map the header column numbers
      for (var k = 0; k < summaryHeader.length; k++){    //Do this for each value in the Header on the summary page

        if (filter.indexOf(summaryHeader[k]) > -1){ // Assign the column number to filter the results on to a variable
           var filterCol = sheetHeader.indexOf(summaryHeader[k])
        }

        if (sheetHeader.indexOf(summaryHeader[k]) == -1){ //If the column name is not in the list assign a temporary value of ZZ
          map.push("ZZ");
        }
         else {
           map.push(sheetHeader.indexOf(summaryHeader[k]));
         }
       
        if (sheetHeader.indexOf(summaryHeader2[k]) > -1){ //If there is a second column header, replace the first mapped number with the second one
         map.splice(k,1,sheetHeader.indexOf(summaryHeader2[k]));
        }    
       
      }



      // Get all the data from the raw sheets
      var sheet = dataSheets[s];
      var rows = sheet.getDataRange();
      var numRows = rows.getNumRows();
      var values = rows.getValues();
      
      // Get only the data I want
      for (var i = 1; i < numRows; i++) {
        var row = values[i];  
        if (row[filterCol] != ""){ //Filter the results by checking to see if the filterCol is empty
        
        
        var preprocessed = [];
                
        for (var n = 0; n < summaryHeader.length; n++){
          
          if (map[n] == "ZZ"){ //If the value is ZZ then enter nothing in the cell
            preprocessed.push("");
          }
            
            else if (typeof row[map[n]] === 'string' && row[map[n]].search(/Other/i) > -1){ // Check if the cell contains other and then concatenate this cell and the next one
              var cell = (row[map[n]].replace("Other","") + " " + row[map[n]+1])
              preprocessed.push(cell)
              //}
            }
            
            else {
              preprocessed.push(row[map[n]]);
            }
         } 
         
         // if (preprocessed[station].search(/test/i) > -1) continue; //don't include test data in the output. 
     
         // Push the data row by row
         processed.push(preprocessed);
         }
       }
      
       // Print out the data sheet by sheet
      if (processed != ""){
       var range = summarySheet.getRange(j,1,processed.length,summaryHeader.length);
       range.setValues(processed); //Set the value
       j = j+processed.length;   
      } 
     }
    }
  }
      
    //Remove duplicate rows
    var data = summarySheet.getDataRange().getValues();
    var newData = new Array();
    newData.push(data[0]);
    newData.push(data[1]);
    for(var i=2; i < data.length;i++){
      if(data[i].join() != data[i-1].join()){
        newData.push(data[i]);
      }
    }
    summarySheet.clearContents();
    summarySheet.getRange(1, 1, newData.length, newData[0].length).setValues(newData);
 // } 
// }
}


function onOpen(e) {
  var spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
  var entries = [{
    
    name : "Run Summary Script",
    functionName : "Summary"

  }];
  spreadsheet.addMenu("Summaries", entries);
};
