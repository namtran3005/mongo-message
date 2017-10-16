/* @flow */
import BPromise from 'bluebird'
import mongoose from 'mongoose'
import EventEmitter from 'events'
import crypto from 'crypto'

type MongoSMQ$options = {
    host?: string,
    db?: string,
    port?: number,
    options?: any,
    client?: ?string,
    ns?: string,
    visibility?: number,
    colName: string,
};

// some helper functions
function id () {
  return crypto.randomBytes(16).toString('hex')
}

function now () {
  return (new Date()).toISOString()
}

function nowPlusSecs (secs: number) {
  return (new Date(Date.now() + (secs * 1000))).toISOString()
}

mongoose.Promise = BPromise

const { Schema } = mongoose
const MessageSchema = new Schema({
  message: Schema.Types.Mixed,
  visible: Schema.Types.Date,
  ack: Schema.Types.String,
  tries: Schema.Types.Number
}, {
  timestamps: true
})
export default class MongoSMQ extends EventEmitter {
  options: MongoSMQ$options;
  mongo: Mongoose$Connection;
  Message: Class<Mongoose$Model>;

  constructor (options: MongoSMQ$options = {}) {
    super()
    const opts = Object.assign({}, {
      host: 'localhost',
      db: 'mongoSMQ',
      port: 27017,
      options: {},
      client: null,
      ns: 'rsmq',
      visibility: 30,
      colName: 'SMQMessage'
    }, options)
    this.options = opts
  }

  async init () {
    const {
      host = '', port = '', db = '', colName
    } = this.options
    let theConnection: Mongoose$Connection = await mongoose.connect(
      `mongodb://${host}:${port}/${db}`,
      {
        useMongoClient: true
      }
    )
    if (theConnection) {
      this.mongo = theConnection
      this.Message = this.mongo.model(colName, MessageSchema)
    }
    return this
  }

  deinit (): Promise<Mongoose$Connection> {
    return this.mongo.close()
  }

  createMessage (payload: mixed): Promise<mixed> {
    const { Message } = this
    const newMsg = new Message({
      message: payload,
      visible: now()
    })
    return newMsg.save()
  }

  getMessage (payload?: mixed, opts?: {visibility: number}): Promise<any> {
    const { Message } = this
    const visibility = (opts && opts.visibility !== undefined)
      ? opts.visibility : this.options.visibility
    const query = {
      deleted: null,
      visible: { $lte: now() }
    }
    const sort = {
      _id: 1,
      visible: 1
    }
    const update = {
      $inc: { tries: 1 },
      $set: {
        ack: id(),
        visible: nowPlusSecs(visibility || 0)
      }
    }
    return Message.findOneAndUpdate(query, update, { sort, new: true }).then()
  }

  updateMessage (payload: {
    _id : string,
    ack : string,
    tries : number,
    message : {
      result : mixed
    }
  }): Promise<any> {
    const { Message } = this
    const { _id, ack, tries, message: {result} } = payload
    const query = {
      _id,
      ack,
      tries
    }
    const update = {
      $set: {
        'message.result': result
      }
    }
    return Message.findOneAndUpdate(query, update, { new: true }).then()
  }

  removeMessageById ({ _id, ack }: { _id: ?string, ack: ?string }): Promise<mixed> {
    const { Message } = this
    const query = {
      _id,
      ack
    }
    /* For ack value,
    ** If it null we mean we looking for object with ack property is null or not exist
    ** If it undefined we mean we don't care about value of ack when find
    */
    if (ack === undefined) {
      delete query.ack
    }
    return Message.findOneAndRemove(query).then()
  }

  total (): Promise<number> {
    const { Message } = this
    return Message.count().then()
  }

  size (): Promise<number> {
    const { Message } = this
    const query = {
      visible: { $lte: now() }
    }
    return Message.count(query).then()
  }

  inFlight (): Promise<number> {
    const { Message } = this
    const query = {
      ack: { $exists: true },
      visible: { $gt: now() }
    }
    return Message.count(query).then()
  }

  clean () {
    const { Message } = this
    return Message.deleteMany().then()
  }
}
