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

// CPUのスレッド数（論理プロセッサ数）を指定
const threads = 24;
// 開始台帳番号と終了台帳番号を指定
const startLedgerIndex = 32570;
const endLedgerIndex = 4184823;
// 保存先を指定
const path = "/var/tmp";

const split = Math.floor((endLedgerIndex - startLedgerIndex) / threads);
console.log("Each thread processes " + split);

const createCsvWriter = require("csv-writer").createObjectCsvWriter;
const rpcClient = new RpcClient<LedgerService>({
  url: "https://s1.ripple.com:51234/",
});

const headers = [
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
];
const headerWithTitle = [
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
];

for (let i = 0; i < threads; i++) {
  ledger(i);
}

function ledger(thread: number) {
  const start = startLedgerIndex + thread * split;
  const end =
    thread == threads - 1
      ? endLedgerIndex
      : startLedgerIndex + (thread + 1) * split - 1;
  const csvWriter = createCsvWriter({
    // 保存する先のパス(すでにファイルがある場合は上書き保存)
    path: path + "/ledger" + start + "-" + end + ".csv",
    // 出力する項目(ここにない項目はスキップされる)
    header: headers,
    append: true,
  });

  const csvWriterWithHeader = createCsvWriter({
    path: path + "/ledger" + start + "-" + end + ".csv",
    header: headerWithTitle,
  });

  get(start)
    // エラーの場合3回トライ
    .catch((err) => {
      console.error("Ledger #" + start + "1st error" + err.code);
      delay(10);
      return get(startLedgerIndex + thread * split);
    })
    .catch((err) => {
      console.error("Ledger #" + start + "2nd error" + err.code);
      delay(10);
      return get(startLedgerIndex + thread * split);
    })
    .catch((err) => {
      console.error("Ledger #" + start + "3rd error" + err.code);
      console.log("Ledger #" + start + " is not registered.");
      return;
    })
    .then(async (result) => {
      await csvWriterWithHeader
        .writeRecords(result?.data.result.ledger.transactions)
        .then(async () => {
          console.log("Writing ledger #" + start + "~");

          for (let i = start + 1; i <= end; i++) {
            await get(i)
              // エラーの場合5回トライ
              .catch((err) => {
                console.error("ledger #" + i + " 1st error" + err.code);
                delay(10);
                return get(i);
              })
              .catch((err) => {
                console.error("ledger #" + i + " 2nd error" + err.code);
                delay(10);
                return get(i);
              })
              .catch((err) => {
                console.error("ledger #" + i + " 3rd error" + err.code);
                delay(10);
                return get(i);
              })
              .catch((err) => {
                console.error("ledger #" + i + " 4th error" + err.code);
                delay(10);
                return get(i);
              })
              .catch((err) => {
                console.error("ledger #" + i + " 5th error" + err.code);
                return;
              })
              .then(async (result) => {
                if (result) {
                  await csvWriter.writeRecords(
                    result?.data.result.ledger.transactions
                  );
                  // .then(() => {
                  //   console.log("Write ledger #" + i);
                  // });
                } else {
                  console.log("Error Detected Ledger #" + i);
                }
              });
          }
        });
      console.log(
        "Complete! saved at /var/tmp/ledger" +
          startLedgerIndex +
          "-" +
          startLedgerIndex +
          split +
          ".csv"
      );
    });
}

async function get(ledgerIndex: number) {
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

function delay(second: number) {
  return new Promise(function (resolve) {
    setTimeout(resolve, second * 1000);
  });
}
