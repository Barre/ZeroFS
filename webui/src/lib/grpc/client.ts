import { createClient } from "@connectrpc/connect";
import { createGrpcWebTransport } from "@connectrpc/connect-web";
import { AdminService } from "./gen/admin_pb";

const transport = createGrpcWebTransport({
  baseUrl: window.location.origin,
});

export const adminClient = createClient(AdminService, transport);
