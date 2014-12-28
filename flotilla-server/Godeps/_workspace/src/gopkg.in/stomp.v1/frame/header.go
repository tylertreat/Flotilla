package frame

// STOMP header names. Some of the header
// names have commands with the same name
// (eg Ack, Message, Receipt). Commands use
// an upper-case naming convention, header
// names use pascal-case naming convention.
const (
	ContentLength = "content-length"
	ContentType   = "content-type"
	Receipt       = "receipt"
	AcceptVersion = "accept-version"
	Host          = "host"
	Version       = "version"
	Login         = "login"
	Passcode      = "passcode"
	HeartBeat     = "heart-beat"
	Session       = "session"
	Server        = "server"
	Destination   = "destination"
	Id            = "id"
	Ack           = "ack"
	Transaction   = "transaction"
	ReceiptId     = "receipt-id"
	Subscription  = "subscription"
	MessageId     = "message-id"
	Message       = "message"
)
