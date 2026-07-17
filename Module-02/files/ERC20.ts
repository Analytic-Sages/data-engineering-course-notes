import { indexer, type Transfer } from "envio";

indexer.onEvent(
  { contract: "ERC20", event: "Transfer" },
  async ({ event, context }) => {
    const transferObject: Transfer = {
      id: `${event.transaction.hash}-${event.logIndex}`,
      from: event.params.from,
      to: event.params.to,
      value: event.params.value,
      blockNumber: event.block.number,
      txhash: event.transaction.hash,
      blockTimestamp: event.block.timestamp,
      tokenAddress: event.srcAddress,
    };

    // Save the transfer to our database
    context.Transfer.set(transferObject);
  },
);

