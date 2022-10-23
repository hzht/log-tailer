# 11 September 2022
# v0.9
# Lenovo Warranty API. Need to use Lenovo provided token. Output in JSON format. 
# to be used with lenovo_warranty_checker.py for transform and upsert to SQL database.

$headers = New-Object "System.Collections.Generic.Dictionary[[String],[String]]"
$headers.Add("clientID", "xxx") # insert token where xxx is.

$device_list = Get-Content -Path .\allmachines220919.csv # serial numbers, one per line. 

write-output "Device, In Warranty, Purchased, Shipped, Contract, Warranty Start Date, Warranty End" >> .\warranty_check.csv

ForEach ($device in $device_list) { 
    try {
        $reply = Invoke-RestMethod "http://supportapi.lenovo.com/v2.5/warranty?serial=$device" -ContentType "application/JSON" -Headers $headers
        $in_warranty = $reply.InWarranty
        $purchased = $reply.Purchased
        $shipped = $reply.Shipped
        $json = "{`"device`" : `"$device`", `"inwarranty`" : `"$in_warranty`", `"purchased`" : `"$purchased`", `"shipped`" : `"$shipped`", "
        $sub_json = "`"contracts`" : {"
        ForEach ($i in $reply.Warranty) {
            $warranty_type = $i.ID
            # $warranty_Name = $i.Name
            $warranty_Start = $i.Start 
            $warranty_End =  $i.End
            $sub_json = $sub_json + "`"$warranty_type`" : [`"$warranty_Start`", `"$warranty_End`"], "
        }
        $sub_json = $sub_json.Substring(0,$sub_json.Length-2) + "}"
        $json = $json + $sub_json + "}"

        write-output "$json" >> .\warranty_check.json 
    }
    catch {
        write-host "Error:"
        write-host $_
    }
    
}