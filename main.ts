import { RpcClient } from "jsonrpc-ts";

interface LedgerService {
  ledger: [
    {
      ledger_index: string;
      accounts: boolean;
      full: boolean;
      transactions: boolean;
      expand: boolean;
      owner_funds: boolean;
    }
  ];
}

// 開始台帳番号と終了台帳番号を指定
const startLedgerIndex = 32570;
const endLedgerIndex = 33000;

const createCsvWriter = require("csv-writer").createObjectCsvWriter;
const csvWriter = createCsvWriter({
  // 保存する先のパス(すでにファイルがある場合は上書き保存)
  path: "/var/tmp/ledger" + startLedgerIndex + "-" + endLedgerIndex + ".csv",
  // 出力する項目(ここにない項目はスキップされる)
  header: [
    "Account",
    "Fee",
    "Flags",
    "LastLedgerSequence",
    "OfferSequence",
    "Sequence",
    "SigningPubKey",
    "TakerGets",
    "TakerPays",
    "TransactionType",
    "TxnSignature",
    "hash",
  ],
  append: true,
});

const csvWriterWithHeader = createCsvWriter({
  path: "/var/tmp/ledger" + startLedgerIndex + "-" + endLedgerIndex + ".csv",
  header: [
    { id: "Account", title: "Account" },
    { id: "Fee", title: "Fee" },
    { id: "Flags", title: "Flags" },
    { id: "LastLedgerSequence", title: "LastLedgerSequence" },
    { id: "OfferSequence", title: "OfferSequence" },
    { id: "Sequence", title: "Sequence" },
    { id: "SigningPubKey", title: "SigningPubKey" },
    { id: "TakerGets", title: "TakerGets" },
    { id: "TakerPays", title: "TakerPays" },
    { id: "TransactionType", title: "TransactionType" },
    { id: "TxnSignature", title: "TxnSignature" },
    { id: "hash", title: "hash" },
  ],
});

const rpcClient = new RpcClient<LedgerService>({
  url: "https://s1.ripple.com:51234/",
});

async function main(ledgerIndex: number) {
  const result = await rpcClient.makeRequest({
    method: "ledger",
    params: [
      {
        ledger_index: String(ledgerIndex),
        accounts: false,
        full: false,
        transactions: true,
        expand: true,
        owner_funds: false,
      },
    ],
    jsonrpc: "2.0",
  });
  return result;
}

main(startLedgerIndex)
  .catch((err) => {
    console.error(err);
    return undefined;
  })
  .then(async (result) => {
    await csvWriterWithHeader
      .writeRecords(result?.data.result.ledger.transactions)
      .then(async () => {
        console.log("Write ledger #" + startLedgerIndex);

        for (let i = startLedgerIndex + 1; i <= endLedgerIndex; i++) {
          await main(i)
            .catch((err) => {
              console.error(err);
              return undefined;
            })
            .then(async (result) => {
              await csvWriter
                .writeRecords(result?.data.result.ledger.transactions)
                .then(() => {
                  console.log("Write ledger #" + i);
                });
            });
        }
      });
    console.log(
      "Complete! saved at /var/tmp/ledger" +
        startLedgerIndex +
        "-" +
        endLedgerIndex +
        ".csv"
    );
  });
