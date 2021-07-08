package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	socketio "github.com/googollee/go-socket.io"
	fleet "github.com/synerex/proto_fleet"
	geo "github.com/synerex/proto_geography"
	pagent "github.com/synerex/proto_people_agent"
	pt "github.com/synerex/proto_ptransit"
	api "github.com/synerex/synerex_api"
	pbase "github.com/synerex/synerex_proto"
	sxutil "github.com/synerex/synerex_sxutil"
)

// Harmoware Vis-Synerex wiht Layer extension provider provides map information to Web Service through socket.io.
type MapMarker2 struct {
	mtype     int32   `json:"mtype"`
	id        int32   `json:"id"`
	lat       float32 `json:"lat"`
	lon       float32 `json:"lon"`
	angle     float32 `json:"angle"`
	speed     int32   `json:"speed"`
	passenger int32   `json:"passenger"`
	etime     string  `json:"etime"`
	color     string  `json:"color"`
}

var (
	nodesrv         = flag.String("nodesrv", "127.0.0.1:9990", "Node ID Server")
	assetDir        = flag.String("assetdir", "", "set Web client dir")
	mapbox          = flag.String("mapbox", "", "Set Mapbox access token")
	idcolor         = flag.String("idcolor", "idcolor.map", "id-color mapping setting")
	port            = flag.Int("port", 3030, "HarmoVis Ext Provider Listening Port")
	localsx         = flag.String("localsx", "", "Local Synerex Server")
	noskip          = flag.Bool("noskip", false, "Do not skip data")
	mu              = new(sync.Mutex)
	assetsDir       http.FileSystem
	ioserv          *socketio.Server
	sxServerAddress string
	mapboxToken     string
	lastMarkers     map[int32]*MapMarker2 // 過去のマーカ情報の記録（時刻以外の変化を見るため）
	colMap          map[int32]string      // ID から色へのマップ
	ptcount         = 0
)

func init() { // initialize function
	lastMarkers = make(map[int32]*MapMarker2)
	colMap = make(map[int32]string)
}

func toJSON(m map[string]interface{}, utime int64) string {
	s := fmt.Sprintf("{\"mtype\":%d,\"id\":%d,\"time\":%d,\"lat\":%f,\"lon\":%f,\"angle\":%f,\"speed\":%d}",
		0, int(m["vehicle_id"].(float64)), utime, m["coord"].([]interface{})[0].(float64), m["coord"].([]interface{})[1].(float64), m["angle"].(float64), int(m["speed"].(float64)))
	return s
}

// assetsFileHandler for static Data
func assetsFileHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		return
	}

	file := r.URL.Path
	//	log.Printf("Open File '%s'",file)
	if file == "/" {
		file = "/index.html"
	}
	f, err := assetsDir.Open(file)
	if err != nil {
		log.Printf("can't open file %s: %v\n", file, err)
		return
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		log.Printf("can't open file %s: %v\n", file, err)
		return
	}
	http.ServeContent(w, r, file, fi.ModTime(), f)
}

func run_server() *socketio.Server {

	currentRoot, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}

	if *assetDir != "" {
		currentRoot = *assetDir
	}

	d := filepath.Join(currentRoot, "mclient", "build")

	fmt.Printf("Start Server")

	assetsDir = http.Dir(d)
	log.Println("AssetDir:", assetsDir)

	assetsDir = http.Dir(d)
	server := socketio.NewServer(nil)

	server.OnConnect("/", func(s socketio.Conn) error {
		resp := server.JoinRoom("/", "#", s)
		log.Printf("Connected ID: %s from %v with URL:%s  %b", s.ID(), s.RemoteAddr(), s.URL(), resp)
		return nil
	})

	server.OnEvent("/", "#", func(c socketio.Conn) {
		log.Printf("Request! %v", c)
	})

	server.OnEvent("/", "get_mapbox_token", func(c socketio.Conn) {
		log.Printf("Requested mapbox access token")
		mapboxToken = os.Getenv("MAPBOX_ACCESS_TOKEN")
		if *mapbox != "" {
			mapboxToken = *mapbox
		}
		c.Emit("mapbox_token", mapboxToken)
		log.Printf("mapbox-token transferred %s ", mapboxToken)
	})

	server.OnError("/", func(s socketio.Conn, e error) {
		log.Printf("SocketErr ID:%s err %v", s.ID(), s.RemoteAddr(), e)
	})

	server.OnDisconnect("/", func(s socketio.Conn, reason string) {
		resp := server.LeaveRoom("/", "#", s)
		log.Printf("Disconnected ID:%s from %v leave:%b reason:%s", s.ID(), s.RemoteAddr(), resp, reason)
	})

	return server
}

type MapMarker struct {
	mtype int32   `json:"mtype"`
	id    int32   `json:"id"`
	lat   float32 `json:"lat"`
	lon   float32 `json:"lon"`
	angle float32 `json:"angle"`
	speed int32   `json:"speed"`
}

func (m *MapMarker) GetJson() string {
	s := fmt.Sprintf("{\"mtype\":%d,\"id\":%d,\"lat\":%f,\"lon\":%f,\"angle\":%f,\"speed\":%d}",
		m.mtype, m.id, m.lat, m.lon, m.angle, m.speed)
	return s
}

func (m *MapMarker2) GetJson() string {
	s := fmt.Sprintf("{\"mtype\":%d,\"id\":%d,\"lat\":%f,\"lon\":%f,\"angle\":%f,\"speed\":%d,\"passenger\":%d,\"etime\":\"%s\", \"color\":\"%s\"}",
		m.mtype, m.id, m.lat, m.lon, m.angle, m.speed, m.passenger, m.etime, m.color)
	return s
}

func supplyRideCallback(clt *sxutil.SXServiceClient, sp *api.Supply) {
	flt := &fleet.Fleet{}
	err := proto.Unmarshal(sp.Cdata.Entity, flt)
	if err == nil {
		log.Print(flt)
		mm := &MapMarker{
			mtype: int32(0),
			id:    flt.VehicleId,
			lat:   flt.Coord.Lat,
			lon:   flt.Coord.Lon,
			angle: flt.Angle,
			speed: flt.Speed,
		}
		//		jsondata, err := json.Marshal(*mm)
		//		fmt.Println("rcb",mm.GetJson())
		mu.Lock()
		ioserv.BroadcastToNamespace("/", "event", mm.GetJson())
		mu.Unlock()
	}
}

func reconnectClient(client *sxutil.SXServiceClient) {
	mu.Lock() // first make client into nil
	if client.SXClient != nil {
		client.SXClient = nil
		log.Printf("Client reset \n")
	}
	mu.Unlock()
	time.Sleep(5 * time.Second) // wait 5 seconds to reconnect
	mu.Lock()
	if client.SXClient == nil {
		newClt := sxutil.GrpcConnectServer(sxServerAddress)
		if newClt != nil {
			log.Printf("Reconnect server [%s]\n", sxServerAddress)
			client.SXClient = newClt
		}
	} else { // someone may connect!
		log.Printf("Use reconnected server\n", sxServerAddress)
	}
	mu.Unlock()
}

func subscribeRideSupply(client *sxutil.SXServiceClient) {
	for {
		ctx := context.Background() //
		err := client.SubscribeSupply(ctx, supplyRideCallback)
		log.Printf("Error:Supply %s\n", err.Error())
		// we need to restart
		reconnectClient(client)
	}
}

func supplyGeoCallback(clt *sxutil.SXServiceClient, sp *api.Supply) {
	log.Print(sp.SupplyName)
	switch sp.SupplyName {
	case "GeoJson":
		geo := &geo.Geo{}
		log.Printf("GeoJson: %d bytes", len(sp.Cdata.Entity))
		err := proto.Unmarshal(sp.Cdata.Entity, geo)
		if err == nil {
			strjs := string(geo.Data)
			log.Printf("Obtaining %s, id:%d, %s, len:%d ", geo.Type, geo.Id, geo.Label, len(strjs))
			//			log.Printf("Data '%s'", strjs)
			mu.Lock()
			ioserv.BroadcastToNamespace("/", "geojson", strjs)
			mu.Unlock()
		}
	case "Lines":
		geo := &geo.Lines{}
		log.Printf("Lines: %d", len(sp.Cdata.Entity))
		err := proto.Unmarshal(sp.Cdata.Entity, geo)
		if err == nil {

			jsonBytes, _ := json.Marshal(geo.Lines)
			log.Printf("LinesParsed: %d", len(jsonBytes))

			mu.Lock()
			ioserv.BroadcastToNamespace("/", "lines", string(jsonBytes))
			mu.Unlock()
		}
	case "ViewState":
		vs := &geo.ViewState{}
		err := proto.Unmarshal(sp.Cdata.Entity, vs)
		if err == nil {
			jsonBytes, _ := json.Marshal(vs)
			log.Printf("ViewState: %v", string(jsonBytes))

			mu.Lock()
			ioserv.BroadcastToNamespace("/", "viewstate", string(jsonBytes))
			mu.Unlock()
		}

	case "BarGraphs":
		bargraphs := &geo.BarGraphs{}
		err := proto.Unmarshal(sp.Cdata.Entity, bargraphs)
		if err == nil {
			jsonBytes, _ := json.Marshal(bargraphs)
			jsonStr := string(jsonBytes)
			//			log.Printf("BarGraphs: %v", jsonStr)
			mu.Lock()
			ioserv.BroadcastToNamespace("/", "bargraphs", jsonStr)
			mu.Unlock()
		}

	case "ClearMoves":
		cms := &geo.ClearMoves{}
		err := proto.Unmarshal(sp.Cdata.Entity, cms)
		if err == nil {
			jsonBytes, _ := json.Marshal(cms)
			//			log.Printf("ClearMoves: %v", string(jsonBytes))

			mu.Lock()
			ioserv.BroadcastToNamespace("/", "clearMoves", string(jsonBytes))
			mu.Unlock()
		}
	case "Pitch":
		cms := &geo.Pitch{}
		err := proto.Unmarshal(sp.Cdata.Entity, cms)
		if err == nil {
			jsonBytes, _ := json.Marshal(cms)
			//			log.Printf("Pitch: %v", string(jsonBytes))

			mu.Lock()
			ioserv.BroadcastToNamespace("/", "pitch", string(jsonBytes))
			mu.Unlock()
		}
	case "Bearing":
		cms := &geo.Bearing{}
		err := proto.Unmarshal(sp.Cdata.Entity, cms)
		if err == nil {
			jsonBytes, _ := json.Marshal(cms)
			//			log.Printf("Bearing: %v", string(jsonBytes))

			mu.Lock()
			ioserv.BroadcastToNamespace("/", "bearing", string(jsonBytes))
			mu.Unlock()
		}

	case "Arcs":
		cms := &geo.Arcs{}
		err := proto.Unmarshal(sp.Cdata.Entity, cms)
		if err == nil {
			jsonBytes, _ := json.Marshal(cms)
			//			log.Printf("Arcs: %v", string(jsonBytes))
			mu.Lock()
			ioserv.BroadcastToNamespace("/", "arcs", string(jsonBytes))
			mu.Unlock()
		}

	case "ClearArcs":
		log.Printf("clearArc!")
		mu.Lock()
		ioserv.BroadcastToNamespace("/", "clearArcs", string(0))
		mu.Unlock()

	case "Scatters":
		cms := &geo.Scatters{}
		err := proto.Unmarshal(sp.Cdata.Entity, cms)
		if err == nil {
			jsonBytes, _ := json.Marshal(cms)
			//			log.Printf("Scatters: %v", string(jsonBytes))
			mu.Lock()
			ioserv.BroadcastToNamespace("/", "scatters", string(jsonBytes))
			mu.Unlock()
		}

	case "ClearScatters":
		log.Printf("clearScatter!")
		mu.Lock()
		ioserv.BroadcastToNamespace("/", "clearScatters", string(0))
		mu.Unlock()

	case "TopTextLabel":
		//		log.Printf("labelInfo!")
		cms := &geo.TopTextLabel{}
		err := proto.Unmarshal(sp.Cdata.Entity, cms)
		if err == nil {

			jsonBytes, _ := json.Marshal(cms)
			//			log.Printf("LabelInfo: %v", string(jsonBytes))
			mu.Lock()
			ioserv.BroadcastToNamespace("/", "topLabelInfo", string(jsonBytes))
			mu.Unlock()

		}

	case "HarmoVIS":
		cms := &geo.HarmoVIS{}
		err := proto.Unmarshal(sp.Cdata.Entity, cms)
		if err == nil {
			jsonBytes, _ := json.Marshal(cms)
			mu.Lock()
			ioserv.BroadcastToNamespace("/", "harmovis", string(jsonBytes))
			mu.Unlock()

		}
	}

}

func subscribeGeoSupply(client *sxutil.SXServiceClient) {
	for {
		ctx := context.Background() //
		err := client.SubscribeSupply(ctx, supplyGeoCallback)
		log.Printf("Error:Supply %s\n", err.Error())
		// we need to restart
		reconnectClient(client)

	}
}

func hsin(theta float64) float64 {
	return math.Pow(math.Sin(theta/2), 2)
}

// Distance function returns the distance (in meters) between two points of
//     a given longitude and latitude relatively accurately (using a spherical
//     approximation of the Earth) through the Haversin Distance Formula for
//     great arc distance on a sphere with accuracy for small distances
//
// point coordinates are supplied in degrees and converted into rad. in the func
//
// distance returned is METERS!!!!!!
// http://en.wikipedia.org/wiki/Haversine_formula
func Distance(lat1, lon1, lat2, lon2 float64) float64 {
	// convert to radians
	// must cast radius as float to multiply later
	var la1, lo1, la2, lo2, r float64
	la1 = lat1 * math.Pi / 180
	lo1 = lon1 * math.Pi / 180
	la2 = lat2 * math.Pi / 180
	lo2 = lon2 * math.Pi / 180

	r = 6378100 // Earth radius in METERS

	// calculate
	h := hsin(la2-la1) + math.Cos(la1)*math.Cos(la2)*hsin(lo2-lo1)

	return 2 * r * math.Asin(math.Sqrt(h))
}

func supplyPTCallback(clt *sxutil.SXServiceClient, sp *api.Supply) {
	pt := &pt.PTService{}
	err := proto.Unmarshal(sp.Cdata.Entity, pt)
	ptcount++

	if err == nil { // get PT
		jsontoken := strings.Split(sp.ArgJson, ",")
		if len(jsontoken) < 26 {
			log.Printf("Data Error: small length! %d < 26[%s]", len(jsontoken), sp.ArgJson)
			return // can't process more..
		}
		tstr := strings.Split(jsontoken[7], ".")[0]
		datestr := jsontoken[6] + " " + tstr + " (JST)"

		bording_count_reset, _ := strconv.Atoi(jsontoken[24]) // エンジン起動以後の乗車人数
		getoff_count_reset, _ := strconv.Atoi(jsontoken[25])  // エンジン起動以後の降車人数
		passenger := bording_count_reset - getoff_count_reset
		if passenger < 0 {
			//			log.Printf("Minus passenger!, ID:%d, count:%d, board:%d, getoff:%d", pt.VehicleId, passenger, bording_count_reset ,getoff_count_reset )
			passenger = 0
		} // 乗車人数は 0以上とする。（本来であれば、乗車数のカウントミス）

		lastMarker := lastMarkers[pt.VehicleId]

		if !*noskip {

			if lastMarker != nil && lastMarker.etime == datestr { // no time change
				//	log.Printf("Same time! %d", pt.VehicleId)
				return
			}

			if lastMarker != nil && lastMarker.lat == float32(pt.Lat) &&
				lastMarker.lon == float32(pt.Lon) &&
				lastMarker.angle == pt.Angle &&
				lastMarker.speed == pt.Speed &&
				passenger == int(lastMarker.passenger) { // no change without time
				//			log.Printf("Same data!:%d", pt.VehicleId)
				return
			}
		}
		// set color!
		col, ok := colMap[pt.VehicleId]
		if !ok {
			col = "3cb371" // default green color
			//
			if *noskip && *idcolor != "" {
				log.Printf("Can't find id-color mapping of ID %d", pt.VehicleId)
			}
		}

		// 時刻が近くて(文字列的に1文字違い)
		if lastMarker != nil {
			ss := lastMarker.etime[:len(lastMarker.etime)-7] // "X (JST)"　をはずす
			if ss == datestr[:len(datestr)-7] {
				//	log.Printf("Less time %s %s", lastMarker.etime, datestr)
				dist := Distance(float64(lastMarker.lat), float64(lastMarker.lon), float64(pt.Lat), float64(pt.Lon))
				if dist > 100 { // 100m /秒
					log.Printf("BigJump id:%d, %f m, %s - %s", pt.VehicleId, dist, lastMarker.etime, datestr)
				} else {
					//					log.Printf("Dist id:%d, dist:%f m",pt.VehicleId, dist)
				}
			}
		}

		mm := &MapMarker2{
			mtype:     pt.VehicleType, // depends on type of GTFS: 1 for Subway, 2, for Rail, 3 for bus
			id:        pt.VehicleId,
			lat:       float32(pt.Lat),
			lon:       float32(pt.Lon),
			angle:     pt.Angle,
			speed:     pt.Speed,
			passenger: int32(passenger),
			etime:     datestr,
			color:     col,
		}

		lastMarkers[pt.VehicleId] = mm // record

		mu.Lock()
		if mm.lat > 10 {
			//log.Printf("supplyPTCallback [%s]", mm.GetJson())
			ioserv.BroadcastToNamespace("/", "event", mm.GetJson())
			//log.Printf("count, passenger: %d - %d = %d", bording_count_reset, getoff_count_reset, passenger)
		}
		mu.Unlock()
	}
}

func subscribePTSupply(client *sxutil.SXServiceClient) {
	ctx := context.Background() //
	err := client.SubscribeSupply(ctx, supplyPTCallback)
	log.Printf("Error:Supply %s\n", err.Error())
}

func supplyPAgentCallback(clt *sxutil.SXServiceClient, sp *api.Supply) {
	log.Printf("Agent: %v", *sp)
	switch sp.SupplyName {
	case "Agents":
		agents := &pagent.PAgents{}
		err := proto.Unmarshal(sp.Cdata.Entity, agents)
		//		log.Printf("Agent: %v", *agents)
		if err == nil {
			seconds := sp.Ts.GetSeconds()
			nanos := sp.Ts.GetNanos()

			jsonBytes, err := json.Marshal(agents)
			if err == nil {
				jstr := fmt.Sprintf("{ \"ts\": %d.%03d, \"dt\": %s}", seconds, int(nanos/1000000), string(jsonBytes))
				log.Printf("Lines: %v", jstr)
				mu.Lock()
				ioserv.BroadcastToNamespace("/", "agents", jstr)
				mu.Unlock()
			} else {
				log.Printf("Invalid Agents! %v count %d", err, len(agents.Agents))
				ags := make([]*pagent.PAgent, 0)
				ct := 0
				for i := 0; i < len(agents.Agents); i++ {
					//					log.Printf("Agent: %d, %f, %f , %v", agents.Agents[i].Id, agents.Agents[i].Point[0], agents.Agents[i].Point[1], agents.Agents[i])
					_, err2 := json.Marshal(agents.Agents[i])
					if err2 == nil {
						ags = append(ags, agents.Agents[i])
						ct++
					}
				}
				ag2 := &pagent.PAgents{
					Agents: ags,
				}
				jsonBytes, err3 := json.Marshal(ag2)
				if err3 == nil {
					jstr := fmt.Sprintf("{ \"ts\": %d.%03d, \"dt\": %s}", seconds, int(nanos/1000000), string(jsonBytes))
					//				log.Printf("Lines: %v", jstr)
					mu.Lock()
					ioserv.BroadcastToNamespace("/", "agents", jstr)
					mu.Unlock()
				} else {
					log.Printf("Invalid Agents! %v again", err3)

				}

			}
		}
	}

}

func subscribePAgentSupply(client *sxutil.SXServiceClient) {
	for {
		ctx := context.Background() //
		err := client.SubscribeSupply(ctx, supplyPAgentCallback)
		log.Printf("Error:Supply %s\n", err.Error())
		// we need to restart
		reconnectClient(client)
	}
}

func monitorStatus() {
	for {
		ss := fmt.Sprintf("PT:%d vlen:%d, connection:%d", ptcount, len(lastMarkers), ioserv.Count())
		sxutil.SetNodeStatus(int32(runtime.NumGoroutine()), ss)
		time.Sleep(time.Second * 3)
	}
}

func main() {
	log.Printf("HarmovisMap(%s) built %s sha1 %s", sxutil.GitVer, sxutil.BuildTime, sxutil.Sha1Ver)
	flag.Parse()

	// load idcolor
	if *idcolor != "" {
		log.Printf("Start IDColor")
		f, err := os.Open(*idcolor)
		if err != nil {
			log.Fatal("Can't open idcolor file", err)
			return
		}
		defer f.Close()
		fileScanner := bufio.NewScanner(f)
		// read line by line
		for fileScanner.Scan() {
			s := fileScanner.Text()
			if strings.HasPrefix(s, "#") {
				continue
			}
			dt := strings.Split(s, " ")
			if len(dt) < 2 {
				continue
			}
			id, _ := strconv.Atoi(dt[0])
			colMap[int32(id)] = dt[1]
		}
		log.Printf("idcolor")
	}

	channelTypes := []uint32{pbase.RIDE_SHARE, pbase.PEOPLE_AGENT_SVC, pbase.GEOGRAPHIC_SVC, pbase.PT_SERVICE}
	var rerr error
	sxServerAddress, rerr = sxutil.RegisterNode(*nodesrv, "HarmoVisMap", channelTypes, nil)
	if rerr != nil {
		log.Fatal("Can't register node ", rerr)
	}

	if *localsx != "" {
		sxServerAddress = *localsx
	}
	log.Printf("Connecting SynerexServer at [%s]\n", sxServerAddress)

	go sxutil.HandleSigInt()
	sxutil.RegisterDeferFunction(sxutil.UnRegisterNode)

	wg := sync.WaitGroup{} // for syncing other goroutines

	log.Printf("start server")
	ioserv = run_server()
	log.Printf("done server")

	if ioserv == nil {
		os.Exit(1)
	}
	go ioserv.Serve() // !!

	client := sxutil.GrpcConnectServer(sxServerAddress) // if there is server address change, we should do it!

	argJSON := fmt.Sprintf("{Client:Map:RIDE}")
	rideClient := sxutil.NewSXServiceClient(client, pbase.RIDE_SHARE, argJSON)

	argJSON2 := fmt.Sprintf("{Client:Map:PAGENT}")
	pa_client := sxutil.NewSXServiceClient(client, pbase.PEOPLE_AGENT_SVC, argJSON2)

	argJSON3 := fmt.Sprintf("{Client:Map:Geo}")
	geo_client := sxutil.NewSXServiceClient(client, pbase.GEOGRAPHIC_SVC, argJSON3)

	argJSON4 := fmt.Sprintf("{Client:Map:PT}")
	pt_client := sxutil.NewSXServiceClient(client, pbase.PT_SERVICE, argJSON4)

	wg.Add(1)
	go subscribeRideSupply(rideClient)

	go subscribePAgentSupply(pa_client)

	go subscribeGeoSupply(geo_client)

	go subscribePTSupply(pt_client)

	go monitorStatus() // keep status

	serveMux := http.NewServeMux()

	serveMux.Handle("/socket.io/", ioserv)
	serveMux.HandleFunc("/", assetsFileHandler)

	log.Printf("Starting Harmoware-VIS Layers Provider %s on port %d", sxutil.GitVer, *port)
	err := http.ListenAndServe(fmt.Sprintf("0.0.0.0:%d", *port), serveMux)
	if err != nil {
		log.Fatal(err)
	}

	wg.Wait()

}
