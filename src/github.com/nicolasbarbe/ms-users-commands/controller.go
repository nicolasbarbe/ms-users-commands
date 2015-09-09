package main

import ( 
        "github.com/julienschmidt/httprouter"
        "github.com/unrolled/render"
        "gopkg.in/mgo.v2"
        "github.com/nicolasbarbe/kafka"
        "encoding/json" 
        "net/http"
        "time"
        "fmt"
        "log"
)


/** Types **/

// User represents a user in the system which can participate in a discussion
type User struct {
  Id            string     `json:"id"            bson:"_id"`
  FirstName     string     `json:"firstName"     bson:"firstName"`
  LastName      string     `json:"lastName"      bson:"lastName"`
  MemberSince   time.Time  `json:"memberSince"   bson:"memberSince"`
}

// Controller embeds the logic of the microservice
type Controller struct {
  mongo         *mgo.Database
  producer      *kafka.Producer
  renderer      *render.Render
}


func (this *Controller) CreateUser(response http.ResponseWriter, request *http.Request, params httprouter.Params ) {

  // build user from the request
  user := new(User)
  if err := json.NewDecoder(request.Body).Decode(user) ; err != nil {
    this.renderer.JSON(response, 422, "Request body is not a valid JSON")
    return
  }
  
  // persits user
  if err := this.mongo.C(usersCollection).Insert(user) ; err != nil {
    log.Printf("Cannot create document in collection %s : %s", usersCollection, err)
    this.renderer.JSON(response, 500, "Cannot save user")
    return
  }

  // create message
  body, err := json.Marshal(user)
  if err != nil {
    log.Printf("Cannot marshall document : %s", err)
    this.renderer.JSON(response, 500, "Cannot process the request")
    return
  }

  message := append([]byte(fmt.Sprintf("%02d%v",len(userCreated), userCreated)), body ...)

  // send message
  if err := this.producer.SendMessageToTopic(message, usersTopic) ; err != nil {
    this.renderer.JSON(response, http.StatusInternalServerError, "Failed to send the message: " + err.Error())
    return
  } 

  // render the response
  this.renderer.JSON(response, 200, "ok")
}






