import { Single } from "rsocket-flowable";
import { Map } from "@shared/map_pb";
import { SetupServiceClient } from "@shared/service_grpc_web_pb";
import { Empty } from "google-protobuf/google/protobuf/empty_pb";
import { ClientReadableStream } from "grpc-web";
import uuid = require("uuid");

export default class SetupServiceClientAdapter {

    private service: SetupServiceClient;

    constructor() {
        this.service = new SetupServiceClient("http://localhost:8000", {}, {});
    }

    map(): Single<Map.AsObject> {
        return new Single(subject => {
            let stream: ClientReadableStream<any>;
            const myId = uuid.v4();
            localStorage.setItem("uuid", myId)
            subject.onSubscribe(() => stream.cancel());
            stream = this.service.get(new Empty(), {"uuid": myId}, (err, response) => {
                if (err) {
                    subject.onError(new Error(`An Grpc Error was thrown. Code: [${err.code}]. Message: ${err.message}`));
                    return;
                }
                subject.onComplete((response.toObject() as any) as Map.AsObject);
            });
        });
    }
}