const { chunk } = require('lodash');
const { from = 0, to = 100_000 } = require('minimist')(process.argv.slice(2));
const READ_CHUNK = 10_000;
const UPDATE_CHUNK_SIZE = 2000;

(async () => {
  try {
    console.time('Script took');
    const col = await getCollection();
    const projection = {
      votes_up: 1,
      votes_funny: 1,
      comment_count: 1,
    };

    let nextId = from;
    let totalOperations = 0;

    console.log('Started reading...');
    while (nextId < to) {
      console.time('Get took');
      const docs = await col
        .find({ _id: { $gt: nextId } }, projection)
        .limit(READ_CHUNK)
        .toArray();
      console.timeEnd('Get took');

      const newNextId = getNextId(docs);
      totalOperations += docs.length;

      console.time('Parallel update took');
      const bulkChunks = chunk(docs.map(getBulkOperations), UPDATE_CHUNK_SIZE);
      await Promise.all(
        bulkChunks.map((operations) => col.bulkWrite(operations))
      );
      console.timeEnd('Parallel update took');

      console.log(`Done ${(totalOperations * 100) / to} %`);

      if (!newNextId || totalOperations >= to) {
        break;
      }

      nextId = newNextId;
    }

    console.timeEnd('Script took');
    process.exit();
  } catch (e) {
    console.timeEnd('Script took');
    console.error(e);
    process.exit(1);
  }
})();

function
 
getNextId(docs) {
  if (!docs.length) {
    return
 
null;
  }
  const nextId = docs[docs.length - 1]._id;
  return nextId || null;
}

function
 
getBulkOperations(record) {
  return {
    updateOne: {
      filter: { _id: record._id },
      update: {
        $set: {
          popularity: record.votes_up + record.votes_funny + record.comment_count,
        },
      },
    },
  };
}
