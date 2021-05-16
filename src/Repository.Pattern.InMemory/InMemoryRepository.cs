using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Repository.Pattern.Abstractions;
using Repository.Pattern.Abstractions.Batches;
using Repository.Pattern.Abstractions.Exceptions;
using Repository.Pattern.Abstractions.Exceptions.Models;

namespace Repository.Pattern.InMemory
{
    /// <summary>
    /// In memory implementation of the repository pattern using a ConcurrentDictionary
    /// </summary>
    /// <typeparam name="T">A Mode object not related with Table storage at all</typeparam>
    public abstract class InMemoryRepository<TDomainModel> : IRepository<TDomainModel> where TDomainModel : class, IDomainModel, new()
    {
        protected readonly ConcurrentDictionary<string, ConcurrentDictionary<string, TDomainModel>> _repository
            = new ConcurrentDictionary<string, ConcurrentDictionary<string, TDomainModel>>();

        public Task<IEnumerable<TDomainModel>> GetAllAsync()
        {
            var result = _repository.SelectMany(r => r.Value.Values).ToList();
            return Task.FromResult(result as IEnumerable<TDomainModel>);
        }

        public Task<IEnumerable<TDomainModel>> GetAllAsync(string partitionKey)
        {
            var partitionKeyValues = GetValue(partitionKey, _repository);

            return Task.FromResult(partitionKeyValues.Values as IEnumerable<TDomainModel>);
        }

        public Task<TDomainModel> GetAsync(string partitionKey, string rowKey)
        {
            var partitionKeyValues = GetValue(partitionKey, _repository);
            var rowKeyValue = GetValue(rowKey, partitionKeyValues);

            return Task.FromResult(rowKeyValue);
        }

        public Task<TDomainModel> AddAsync(TDomainModel domainModel)
        {
            var partitionKeyValues = GetValue(domainModel.PartitionKey, _repository);
            var success = partitionKeyValues.TryAdd(domainModel.RowKey, domainModel);

            if (!success)
            {
                throw new AlreadyExistsException(nameof(AddAsync))
                {
                    DomainModelUids = BuildDomainModelUids(domainModel)
                };
            }

            return Task.FromResult(domainModel);
        }

        public async Task AddBatchAsync(IEnumerable<TDomainModel> domainModelEnumerable, BatchOperationOptions options = null)
        {
            var domainModels = domainModelEnumerable.ToList();
            Func<TDomainModel, Task<TDomainModel>> operation = (dm) => AddOrUpdateAsync(dm);

            if (options != null)
            {
                switch (options.BatchInsertMethod)
                {
                    case BatchInsertMethod.Insert:
                        var existingDomainModels = domainModels.Where(dm => Exists(dm)).ToArray();
                        if (existingDomainModels.Any())
                        {
                            throw new AlreadyExistsException(nameof(AddBatchAsync))
                            {
                                DomainModelUids = BuildDomainModelUids(existingDomainModels)
                            };
                        }
                        operation = (dm) => AddAsync(dm);
                        break;
                        // We will implement both the same way, like an upsert
                        //case BatchInsertMethod.InsertOrMerge:
                        //case BatchInsertMethod.InsertOrReplace:
                }
            }

            foreach (var domainModel in domainModelEnumerable)
            {
                await operation(domainModel);
            }

            return;
        }

        public Task<TDomainModel> AddOrUpdateAsync(TDomainModel domainModel)
        {
            var partitionKeyValues = GetValue(domainModel.PartitionKey, _repository, nameof(AddOrUpdateAsync));
            var result = partitionKeyValues.AddOrUpdate(domainModel.RowKey, domainModel, (s, d) => d);

            return Task.FromResult(result);
        }

        public Task<TDomainModel> UpdateAsync(TDomainModel domainModel)
        {
            var partitionKeyValues = GetValue(domainModel.PartitionKey, _repository, nameof(UpdateAsync));
            var success = partitionKeyValues.TryUpdate(domainModel.RowKey, domainModel, domainModel);

            if (!success)
            {
                throw new DoesNotExistsException(nameof(UpdateAsync))
                {
                    DomainModelUids = BuildDomainModelUids(domainModel)
                };
            }

            return Task.FromResult(domainModel);
        }

        public Task DeleteAllAsync(string partitionKey)
        {
            _ = _repository.TryRemove(partitionKey, out _);
            return Task.CompletedTask;
        }

        public Task<TDomainModel> DeleteAsync(TDomainModel domainModel)
        {
            return DeleteAsync(domainModel.PartitionKey, domainModel.RowKey);
        }

        public Task<TDomainModel> DeleteAsync(string partitionKey, string rowKey)
        {
            var partitionKeyValues = GetValue(partitionKey, _repository, nameof(DeleteAsync));
            var success = partitionKeyValues.TryRemove(rowKey, out TDomainModel deleted);

            if (!success)
            {
                throw new DoesNotExistsException(nameof(DeleteAsync))
                {
                    DomainModelUids = new[] { new DomainModelUid()
                    {
                        PartitionKey = partitionKey,
                        RowKey = rowKey
                    }}
                };
            }

            return Task.FromResult(deleted);
        }        

        private bool Exists(TDomainModel domainModel)
        {
            var partitionKeyValues = GetValue(domainModel.PartitionKey, 
                _repository, throwException: false);

            if (partitionKeyValues == null)
            {
                return false;
            }

            var rowKeyValue = GetValue(domainModel.RowKey, 
                partitionKeyValues, throwException: false);

            return rowKeyValue != null;
        }

        private T GetValue<T>(string key, ConcurrentDictionary<string, T> dictionary, 
            string callerName = nameof(GetValue), bool throwException = true)
        {
            var exists = dictionary.TryGetValue(key, out T entries);

            if (throwException && !exists)
            {
                throw new DoesNotExistsException($"{callerName}: {key}");
            }

            return entries;
        }

        private DomainModelUid[] BuildDomainModelUids(params IDomainModel[] domainModels)
        {
            return domainModels.Select(dm => new DomainModelUid()
            {
                PartitionKey = dm.PartitionKey,
                RowKey = dm.RowKey
            }).ToArray();
        }
    }
}
