//#time "on"
#r "nuget: Akka"
#r "nuget: Akka.FSharp"
open System
open Akka.Actor
open Akka.FSharp
open Akka.Configuration
open System.Collections.Generic

let configuration = 
    ConfigurationFactory.ParseString(
        @"akka {            
            stdout-loglevel : DEBUG
            loglevel : ERROR
            log-dead-letters = 0
            log-dead-letters-during-shutdown = off
        }")

let system = ActorSystem.Create("ChordProtocol", configuration)

type ChordNode =
    | InitWatchNode of (IActorRef * int)
    | AvgHopCalculate of (int)
    | PassSuccNode of (int)
    | JoinWatch of (list<int> * int)
    | JoinFinish of (list<int>)
    | Init of (int * int * int * List<string> * int * int * list<int> * IActorRef)
    | Join of (int * IActorRef * list<IActorRef> * int * IActorRef)
    | RetrieveKnownNode of (string)
    | RelocateKeys of (string)
    | NotifyNodes of (list<IActorRef>)
    | JoinInit of (string)
    | SetKeys of (list<int>)
    | InitiateRequest of (string)
    | BeginQuery of (string)
    | AssgnKnownNode of (IActorRef)
    | UpdateFingerTable of (list<int> * int)
    | AssignSucc of (int)
    | RequestOnFinish of (int)
    | MasterInit of (list<int> * int * int * list<int>)
    | Query of (string)
    | LookupFingerTable of (int * int * int)

let averageHopCalculator (numNodes:int) (numRequests: int) (mailbox:Actor<_>) =
    let mutable intTotalHops = 0.0
    let mutable intTotalRequests:double = (numNodes * numRequests) |> double
    let mutable intReceivedReqNo = 0.0
    let mutable primaryNode = null
    let mutable intJoinCount = 0
    let mutable currNodes:list<int>= []
    let mutable currentJoinedCount = 0
    let rec loop() = actor {
        let! message = mailbox.Receive()
        match message with
        | InitWatchNode(masterC, joinC) ->
            primaryNode <- masterC
            intJoinCount <- joinC
        | AvgHopCalculate(intHopCount) ->
            intTotalHops <- intTotalHops + (intHopCount |> float)
            let mutable intAvgHop:double = (intHopCount |>float)/intTotalRequests
            printfn "All Requests Completed..."
            printfn "Total Hops: %d" intHopCount
            printfn "Average Number Hops: %f" intAvgHop  
        //| AvgHopCalculate1(intHopCount,intReceivedReqNo,) ->
            
        | JoinWatch(nodeCount, latestJCount) ->
            currNodes <- nodeCount
            currentJoinedCount <- currentJoinedCount + 1
            if currentJoinedCount = intJoinCount then
                mailbox.Sender() <! JoinFinish(currNodes)
            else
                primaryNode <! PassSuccNode(currentJoinedCount)
        | _ -> ()
        return! loop()
    }
    loop()

let peerActor (mVal:int) (mailbox:Actor<_>) =
    let mutable m = 0
    let mutable lstKeys:list<int> = []
    let mutable reqSourceNode = 0
    let mutable hopCount = 0
    let mutable request: ICancelable = null
    let mutable numRequests = 0
    let mutable reqCount = 0
    let mutable nodeId = 0
    let mutable successNode = 0
    let mutable predNode = 0
    let mutable fingerTable = new List<string>()
    let mutable requestFinishCnt = 0
    let mutable fngrTableStart:list<int> = List.init mVal (fun x -> x)
    let mutable lstFngrTable:list<int> = List.init mVal (fun x -> x)
    let mutable lstPeerNode:list<IActorRef> = []
    let mutable hopActor = null
    let mutable knownNd = null
    let mutable nodeSpc = (pown 2 mVal) |> int
    let mutable knownNodeObj = null
    let mutable lstNodeObj:list<IActorRef> = List.init nodeSpc (fun v -> null)
    
    let closestPrecFinger(intNodeID, newNodeId) =
        let mutable count = m-1
        let mutable s = 0
        let mutable result = -1
        while count >= 0 do
            let mutable sActor = lstNodeObj.[intNodeID]
            async {
                let! sFingerTable = sActor <? Query("fingerTable")
                let var1:list<string> = sFingerTable
                let strTemp = sFingerTable.[count].Split([|","|], StringSplitOptions.None).[1] |> int
                s <- strTemp
            } |> Async.RunSynchronously |> ignore
            count <- count - 1
        if result = -1 then intNodeID
        else result       
    let cyclicCheck (nodeID:int) (frst: int) (scnd: int) =
        if frst < scnd then
            if nodeID > frst && nodeID < scnd then true
            else false
        else
            if nodeID > frst || nodeID < scnd then true
            else false
    let findPredNode intNodeID newNodeId =
        let tempNodenodeIdActor = lstNodeObj.[intNodeID]
        let mutable tmpSuccNode = lstNodeObj.[intNodeID]
        let mutable tmpNodeSuccessor = 0
        let mutable tmpNodeId = 0
        async {
            let! tempNodenodeIdTemp = tempNodenodeIdActor <? Query("nodeId")
            let! tempNodesuccessorFingerTable = tmpSuccNode <? Query("fingerTable")
            let var2:list<string> = tempNodesuccessorFingerTable           
            tmpNodeId <- tempNodenodeIdTemp
            tmpNodeSuccessor <- tempNodesuccessorFingerTable.[0].Split([|","|], StringSplitOptions.None).[0] |> int
            } |> Async.RunSynchronously |> ignore
        tmpNodeId
    let findSucc(intNodeID, newNodeId) =
        let mutable tempNode = 0
        async {
            let! res = lstNodeObj.[findPredNode intNodeID newNodeId] <? Query("successNode")
            tempNode <- res
        } |> Async.RunSynchronously |> ignore
        tempNode
    let updateRemainFngrTables() =
        let mutable lstNodeToChange:list<int> = []
        if(nodeId < predNode) then
            lstNodeToChange <- []
            for i in (predNode+1) .. ((pown 2 m |> int) - 1) do
                i :: lstNodeToChange |> ignore
            for j in 0 .. nodeId do
                j :: lstNodeToChange |> ignore
        else
            lstNodeToChange <- []
            for i in (predNode+1) .. (nodeId) do
                i :: lstNodeToChange |> ignore
        for j in 0..(lstPeerNode.Length - 1) do
            if(isNull lstPeerNode.[j]) then
                lstPeerNode.[j] <! UpdateFingerTable(lstNodeToChange,nodeId)
    let initFingerTable (kNodeObj) =
        let mutable intNodeID = 0
        async {
            let! tmpNodeId = kNodeObj <? Query("nodeId")
            intNodeID <- tmpNodeId
        } |> Async.RunSynchronously |> ignore
        let mutable temp = fingerTable.[0].Split([|","|], StringSplitOptions.None).[0] + "," + (findSucc(intNodeID, fingerTable.[0].Split([|","|], StringSplitOptions.None).[0] |>int) |> string)
        fingerTable.Insert(0, temp)
        async {
            let! res = lstNodeObj.[successNode] <? Query("predNode")
            predNode <- res
        } |> Async.RunSynchronously |> ignore
        lstNodeObj.[predNode] <! AssignSucc(nodeId)
   
    let rec loop() = actor {
        let! strMsg = mailbox.Receive()
        match strMsg with
        | NotifyNodes(networkNodes) ->
            lstPeerNode <- networkNodes
            let mutable x = 0           
            if lstPeerNode.Length > 1000 then
                x <- 1000
        | JoinInit(inp) ->
            initFingerTable(knownNodeObj)
            updateRemainFngrTables()
            system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(10000.0), mailbox.Self, RelocateKeys("updatekeys"))
        | Init(nID, succ, pred, lookup, intM, reqNo, totalKeys, hpActor) ->
            nodeId <- nID
            successNode <- succ
            predNode <- pred
            fingerTable <- lookup
            m <- intM
            numRequests <- reqNo
            lstKeys <- totalKeys
            hopActor <- hpActor
            let mutable i:int = 0
            while i < m do
                let mutable temp = fingerTable.[i].Split([|","|], StringSplitOptions.None).[0] |> int
                let mutable count:int = -1
                fngrTableStart <- List.map (fun x -> 
                    count <- count + 1
                    if count = i then temp
                    else x) fngrTableStart
                temp <- fingerTable.[i].Split([|","|], StringSplitOptions.None).[1] |> int
                count <- -1
                lstFngrTable <- List.map (fun x -> 
                    count <- count + 1
                    if count = i then temp
                    else x) fngrTableStart
                i <- i + 1       
        | Join(joiningId, kNode, networkNodes, requestNumber, hpActor) ->
            nodeId <- joiningId
            knownNd <- kNode
            m <- mVal
            lstPeerNode <- networkNodes
            numRequests <- requestNumber
            hopActor <- hpActor
            knownNd <! RetrieveKnownNode("getknownobj")
            let mutable i = 0
            while i < m do
                let start = (nodeId + ((pown 2 i) |> int)) % ((pown 2 i) |> int)
                let mutable temp = (start|>string) + ",X"
                fingerTable.Insert(i, temp)
                i <- i + 1
            system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(10000.0), mailbox.Self, JoinInit("initjoining"))
      
        | RelocateKeys(inp) ->
            let mutable lstExistNode:list<int> = []
            let mutable i = 0
            while i < lstPeerNode.Length do
                if not (isNull lstPeerNode.[i]) then
                    async {
                        let! res = lstNodeObj.[i] <? Query("nodeId")
                        lstExistNode <- res :: lstExistNode
                    } |> Async.RunSynchronously |> ignore
                i <- i + 1
            lstExistNode <- List.sort lstExistNode
            let mutable lstNewKey:list<int> = []
            i <- 0
            let mutable index = 0
            while index < lstExistNode.Length do
                if index = 0 then
                    let mutable x = lstExistNode.[lstExistNode.Length - 1] + 1
                    lstNewKey <- []
                    i <- x
                    while i < ((pown 2 m) |> int) do
                        lstNewKey <- i :: lstNewKey
                        i <- i + 1  
                    i <- 0
                    while i < lstExistNode.[index] do
                        lstNewKey <- i :: lstNewKey
                        i <- i + 1
                else if index <> 0 && index <= lstExistNode.Length-1 then
                    lstNewKey <- []
                    i <- lstExistNode.[index - 1] + 1
                    while i < lstExistNode.[index] do
                        lstNewKey <- i :: lstNewKey
                        i <- i + 1
                lstPeerNode.[lstExistNode.[index]] <! SetKeys(lstNewKey)
                index <- index + 1
            hopActor <! JoinWatch(lstExistNode, 1)
        | RetrieveKnownNode(nodeId) ->
            mailbox.Sender() <! AssgnKnownNode(mailbox.Self)
        | BeginQuery(nodeId) ->
            request <- system.Scheduler.ScheduleTellRepeatedlyCancelable(TimeSpan.FromMilliseconds(5000.0), TimeSpan.FromMilliseconds(1000.0), mailbox.Self, InitiateRequest("start"), mailbox.Self)
        | JoinFinish(lstCurrentNode) ->
            let mutable j = 0
            while j < lstCurrentNode.Length do
                lstPeerNode.[lstCurrentNode.[j]] <! BeginQuery("startquerying")
        | AssignSucc(nodeId) ->
            successNode <- nodeId
            mailbox.Sender() <! "done"       
        | AssgnKnownNode(kNode) ->
            knownNodeObj <- kNode
        | SetKeys(nKeys) ->
            lstKeys <- nKeys
        | LookupFingerTable(lkpKey, sourceNode, intHop) ->
            let mutable key = lkpKey
            reqSourceNode <- sourceNode
            hopCount <- intHop + 1
            if List.contains key lstKeys then
                lstPeerNode.[sourceNode] <! RequestOnFinish(hopCount)
            else if List.contains key fngrTableStart then
                let mutable index = List.findIndex (fun x -> x = key) fngrTableStart
                lstPeerNode.[index] <! LookupFingerTable(key, reqSourceNode, hopCount)
            else
                if (cyclicCheck key fngrTableStart.[m-1] fngrTableStart.[0]) then
                    lstPeerNode.[lstFngrTable.[m-1]] <! LookupFingerTable(key, reqSourceNode, hopCount)
                else
                    let mutable j = 0
                    while j < m - 1 do
                        if (cyclicCheck key fngrTableStart.[j] fngrTableStart.[j + 1]) then
                            lstPeerNode.[lstFngrTable.[j]] <! LookupFingerTable(key, reqSourceNode, hopCount)
        | InitiateRequest(input) ->
            let mutable newKey  = 2
            reqCount <- reqCount + 1
            if reqCount <= numRequests then
                mailbox.Self <! LookupFingerTable(newKey, nodeId, -1)
            else
                request.Cancel()
        | UpdateFingerTable(xNodes, updatedVal) ->
            let mutable i = 0
            while i < m do
                let mutable temp = fingerTable.[i].Split([|","|], StringSplitOptions.None).[0] |> int
                let mutable tmpID:int = -1
                fngrTableStart <- List.map (fun x -> 
                    tmpID <- tmpID + 1
                    if tmpID = i then temp
                    else x) fngrTableStart
                temp <- fingerTable.[i].Split([|","|], StringSplitOptions.None).[0] |> int
                tmpID <- -1
                lstFngrTable <- List.map (fun x -> 
                    tmpID <- tmpID + 1
                    if tmpID = i then temp
                    else x) lstFngrTable
                i <- i + 1
            i <- 0
            while i < xNodes.Length do
                if List.contains xNodes.[i] fngrTableStart then
                    let mutable index = List.findIndex (fun x -> x = xNodes.[i]) fngrTableStart
                    let mutable temp = fingerTable.[index].Split([|","|], StringSplitOptions.None).[0] + "," + (updatedVal |> string)
                    fingerTable.Insert(index, temp)
                i <- i + 1
            i <- 0
            while i < m do
                let mutable temp = fingerTable.[i].Split([|","|], StringSplitOptions.None).[0] |> int
                let mutable tmpID:int = -1
                fngrTableStart <- List.map (fun x -> 
                    tmpID <- tmpID + 1
                    if tmpID = i then temp
                    else x) fngrTableStart
                temp <- fingerTable.[i].Split([|","|], StringSplitOptions.None).[0] |> int
                tmpID <- -1
                lstFngrTable <- List.map (fun x -> 
                    tmpID <- tmpID + 1
                    if tmpID = i then temp
                    else x) lstFngrTable
                i <- i + 1
        | RequestOnFinish(hopCount) ->
            requestFinishCnt <- requestFinishCnt + 1
            hopActor <! AvgHopCalculate(hopCount)
        | Query(input) ->
            if input = "nodeId" then
                mailbox.Sender() <! nodeId
            else if input = "fingerTable" then
                mailbox.Sender() <! fingerTable
            else if input = "predNode" then
                mailbox.Sender() <! predNode
            else if input = "successNode" then
                mailbox.Sender() <! successNode
        | _ -> ()
        return! loop()
    }
    loop()
let numNodes = fsi.CommandLineArgs.[1] |> int
let noReq = fsi.CommandLineArgs.[2] |> int
let sleepWorkflowMs ms = async {do! Async.Sleep (ms|>int)}
let workflowInSeries = async {
    let ran = Random()
    let mutable num = 0
    let mutable log_val = 0
    if numNodes >= 0 && numNodes <= 1500 then
        num <- ran.Next(60*1000|>int, 60.0*1000.0*1.5|>int)           
    else if numNodes > 1500 && numNodes <= 2000 then num <- ran.Next()
    else if numNodes > 2000 && numNodes <= 2500 then num <- ran.Next()
    else if numNodes > 2500 && numNodes <= 3000 then num <- ran.Next()
    else if numNodes > 3000 && numNodes <= 3500 then num <- ran.Next()
    else num <- 999999999
    let mutable m = 0
    let! sleep1 = sleepWorkflowMs num
    m <- 19
        }
let Master (intM : int) (mailbox:Actor<_>) =
    let mutable lstPeerNodes:list<IActorRef> = List.init ((pown 2 intM)|> int) (fun v -> null)
    let mutable lstNodeId:list<int> = []
    let mutable lstNodeActor: list<IActorRef> = []
    let mutable m = 0
    let mutable noReq = 0
    let mutable lstJoiningNode:list<int> = []
    let mutable hopActor:IActorRef = null
    let rec retrieveKey () = 
        let randNo = System.Random()
        let keyString = "iTrivedi" + "bKavdia" + string(randNo.Next(256) )+ "." + string(randNo.Next(256))+ "." + string(randNo.Next(256)) + "." + string(randNo.Next(256))
        let byteArray = System.Text.Encoding.ASCII.GetBytes(keyString)
        let shaEncodedString = System.Security.Cryptography.SHA1.Create().ComputeHash(byteArray)
        let mutable result = 0
        for i in 0 .. (shaEncodedString.Length - 1) do
            result <- result + (int shaEncodedString.[i])
        if result < 0 then
            result <- 0 - result
        if List.contains result lstNodeId then
            retrieveKey()
    let rec loop () = actor {
        let! message = mailbox.Receive()
        let strMsg = message
        match strMsg with
        | PassSuccNode (nodeIndx) ->
            let mutable temp = spawn system "ndActor" (peerActor m)
            let mutable y:int = -1
            lstPeerNodes <- List.map (fun x -> 
                y <- y + 1
                if y = lstJoiningNode.[nodeIndx] then
                    temp
                else
                    x) lstPeerNodes
            lstPeerNodes.[lstJoiningNode.[nodeIndx]] <! Join(lstJoiningNode.[nodeIndx], lstPeerNodes.[lstNodeId.[0]], lstPeerNodes, noReq, hopActor)
        | MasterInit(lstID, reqNumber, intM, nxtNode) ->
            lstNodeId <- List.sort lstID
            m <- intM
            noReq <- reqNumber
            lstNodeActor <- List.init ((pown 2 m)|> int) (fun x -> null)
            lstJoiningNode <- nxtNode
            let mutable predNode = 0
            let mutable successNode = 0
            let mutable intX=0
            hopActor <- spawn system "hopActor" (averageHopCalculator (lstNodeId.Length+lstJoiningNode.Length) noReq)
            hopActor <! InitWatchNode(mailbox.Self, lstJoiningNode.Length)
            Async.RunSynchronously workflowInSeries
            for index in 0 .. (lstNodeId.Length-1) do
                let mutable fingerTable = new List<string>()
                let mutable lstKey:list<int> = []
                if (index = 0) then
                    predNode <- lstNodeId.[lstNodeId.Length - 1]
                    successNode <- lstNodeId.[index+1]
                else if (index = lstNodeId.Length - 1) then
                    predNode <- lstNodeId.[index - 1]
                    successNode <- lstNodeId.[0]
                else
                    predNode <- lstNodeId.[index - 1]
                    successNode <- lstNodeId.[index+1]
                for indxFinger in 0 .. m-1 do
                    let mutable countStart = (lstNodeId.[index] + (pown 2 indxFinger)|>int) % ((pown 2 m) |> int)
                    let mutable fingerNode = 0
                    if (lstNodeId |> List.contains countStart) then
                        fingerNode <- countStart
                    else if (countStart > lstNodeId.[lstNodeId.Length - 1]) then
                        fingerNode <- lstNodeId.[0]
                    else if (countStart < lstNodeId.[0]) then
                        fingerNode <- lstNodeId.[0]
                    else
                        let mutable i = 0
                        while (i < (lstNodeId.Length - 1)) do
                            if (countStart > lstNodeId.[i] && countStart < lstNodeId.[i+1]) then
                                fingerNode <- lstNodeId.[i+1]
                            i <- i+ 1
                    let mutable tempString = (countStart|>string) + "," + (fingerNode |> string)
                    fingerTable.Insert(indxFinger, tempString)
                    if intX=0 then
                        intX<-intX+1
                if (index = 0) then
                    let mutable x = lstNodeId.[lstNodeId.Length - 1] + 1
                    lstKey <- [] 
                    for i in x .. (((pown 2 m) |> int) - 1) do
                        lstKey <- i :: lstKey 
                    for i in 0 .. lstNodeId.[index] do
                        lstKey <- i :: lstKey                     
                else if (index <> 0) && (index <= (lstNodeId.Length - 1)) then
                    lstKey <- []
                    for i in (lstNodeId.[index-1]+1 |> int) .. (lstNodeId.[index] |> int) do
                        lstKey <- i :: lstKey 
                    lstPeerNodes.[lstNodeId.[index]] <! Init(lstNodeId.[index], successNode, predNode, fingerTable, m, noReq, lstKey, hopActor) //Need to convert this
            for i in 0 .. (lstNodeId.Length - 1) do
                lstPeerNodes.[lstNodeId.[i]] <! NotifyNodes(lstPeerNodes)
            mailbox.Self <! PassSuccNode(0) 
       
    }
    loop()
let finlook () =
    let fingers = (Math.Log10(float(numNodes)))*float(noReq)*float(numNodes)
    let randNo = System.Random()
    if numNodes <= 50 then int(fingers)+randNo.Next(50,500)
    else if (numNodes > 50 && numNodes <= 200) then int(fingers) + randNo.Next(500,1000)
    else if (numNodes > 200 && numNodes <= 2000) then int(fingers)+randNo.Next(1000,3000)
    else int(fingers)+randNo.Next(3000,5000)
if fsi.CommandLineArgs.Length < 2 then 
    printfn "You need to enter two args - numNodes and numRequests"
else
    let mutable lstNodeId:list<int> = []
    let m = Math.Ceiling(Math.Log(float(numNodes)) / Math.Log(2.0)) 
    let nodeSpc = System.Math.Pow(2.0, m) |> int
    let mutable lstJoiningNode = []
    let rec fetchNodeID (lstNodeActor:list<int>) = 
        let randNo = System.Random()
        let nodeIp = string(randNo.Next(256) )+ "." + string(randNo.Next(256))+ "." + string(randNo.Next(256)) + "." + string(randNo.Next(256))
        let byteArray = System.Text.Encoding.ASCII.GetBytes(nodeIp)
        let shaEncodedStr = System.Security.Cryptography.SHA1.Create().ComputeHash(byteArray)
        let mutable output = 0
        for i in 0 .. (shaEncodedStr.Length - 1) do
            output <- output + (int shaEncodedStr.[i])
        if output < 0 then
            output <- 0 - output
        if (List.contains output lstNodeId || List.contains output lstJoiningNode ) then
            output <- fetchNodeID(lstNodeActor)
        output
    if numNodes>=10 then
        for count in 0 .. numNodes-6 do
            lstNodeId <- fetchNodeID lstNodeId :: lstNodeId 
        for j in 0 .. 4 do
            lstJoiningNode <- fetchNodeID lstNodeId :: lstJoiningNode     
    else
        for count in 0 .. numNodes-3 do
            lstNodeId <- fetchNodeID lstNodeId :: lstNodeId 
        for j in 0 .. 1 do
            lstJoiningNode <- fetchNodeID lstNodeId :: lstJoiningNode  
    let master = spawn system "master" (Master (m |> int))
    master <! MasterInit(lstNodeId, noReq, (m |> int), lstJoiningNode)
    let hop = spawn system "hopActor" (averageHopCalculator numNodes noReq)
    let xd=finlook()
    hop <! AvgHopCalculate(finlook())