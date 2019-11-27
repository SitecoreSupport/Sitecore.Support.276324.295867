using Sitecore.Commerce.Core;
using Sitecore.Commerce.Engine.Connect.DataProvider;
using Sitecore.Commerce.Engine.Connect.Search;
using Sitecore.Commerce.Engine.Connect.Search.Crawlers;
using Sitecore.Commerce.Engine.Connect.Search.Models;
using Sitecore.Commerce.Plugin.Catalog;
using Sitecore.Commerce.Plugin.ManagedLists;
using Sitecore.Configuration;
using Sitecore.ContentSearch;
using Sitecore.ContentSearch.Diagnostics;
using Sitecore.Data;
using Sitecore.Data.Items;
using Sitecore.Data.Managers;
using Sitecore.Diagnostics;
using Sitecore.Events;
using Sitecore.Globalization;
using Sitecore.SecurityModel;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Sitecore.Support.Sitecore.Commerce.Engine.Connect.Search.Crawlers
{
    public abstract class CatalogCrawlerBase<TEntity> : FlatDataCrawler<CommerceCatalogIndexableItem>
            where TEntity : CatalogItemBase
    {
        protected CatalogCrawlerBase()
        {
            this.CurrentCrawlOperations = new ConcurrentDictionary<Guid, CrawlState>();
            this.Repository = new CatalogRepository();
        }

        public int ItemsToTake { get; set; }

        public string ListName { get; set; }

        public string IndexListName { get; set; }

        public List<string> Environments { get; } = new List<string>();

        protected CatalogRepository Repository { get; }

        protected IDictionary<Guid, CrawlState> CurrentCrawlOperations { get; }

        protected CrawlState AddCrawlState { get; set; }

        protected CrawlState UpdateCrawlState { get; set; }

        protected abstract IEnumerable<ID> IndexableTemplateIds { get; }

        public string Database { get; set; }

        protected override void Add(IProviderUpdateContext context, System.Threading.CancellationToken cancellationToken)
        {
            Assert.ArgumentNotNull(context, nameof(context));

            ManagedList searchResults = null;
            int skip = 0;
            var totalCount = 0;

            this.AddCrawlState = new CrawlState(context.ParallelOptions, this.index, cancellationToken);
            this.LogInfo("Checking for catalog items to index in list '{0}'.", this.ListName);

            try
            {
                var crawledArtifactStores = new List<string>();
                foreach (var environment in this.Environments)
                {
                    this.LogInfo("Processing environment '{0}'", environment);

                    var artifactId = IndexUtility.GetEnvironmentArtifactStoreId(environment);
                    if (crawledArtifactStores.Contains(artifactId))
                    {
                        return;
                    }

                    crawledArtifactStores.Add(artifactId);

                    this.CurrentCrawlOperations[this.AddCrawlState.Id] = this.AddCrawlState;

                    var mappedCatalogsCache = new Dictionary<string, bool>();

                    do
                    {
                        searchResults = this.GetCatalogEntitiesToIndex(environment, this.ListName, skip, this.ItemsToTake);
                        if (searchResults == null || !searchResults.Items.Any())
                        {
                            break;
                        }

                        var indexableList = this.GetItemsToIndex(searchResults, mappedCatalogsCache);
                        this.AddItemsToIndex(context, indexableList);
                        IndexUtility.ClearIndex(environment, this.IndexListName, indexableList.Select(item => item.SitecoreId.ToString()).ToList());

                        skip += this.ItemsToTake;
                        totalCount += searchResults.Items.Count;
                    }
                    while ((searchResults != null) && (searchResults.Items.Count >= this.ItemsToTake));
                }
            }
            finally
            {
                this.LogInfo("Indexed {0} entities of type {1} in list '{2}'.", totalCount, typeof(TEntity).Name, this.ListName);
                if (this.CurrentCrawlOperations.ContainsKey(this.AddCrawlState.Id))
                {
                    this.CurrentCrawlOperations.Remove(this.AddCrawlState.Id);
                }
            }
        }

        protected virtual List<CommerceCatalogIndexableItem> GetItemsToIndex(ManagedList searchResults, Dictionary<string, bool> mappedCatalogsCache)
        {
            Assert.ArgumentNotNull(searchResults, nameof(searchResults));
            Assert.ArgumentNotNull(mappedCatalogsCache, nameof(mappedCatalogsCache));

            var indexableList = new List<CommerceCatalogIndexableItem>();
            foreach (var entity in searchResults.Items.OfType<TEntity>())
            {
                var parentCatalogSitecoreIdList = entity.ParentCatalogList.SplitPipedList();
                foreach (var parentCatalogSitecoreId in parentCatalogSitecoreIdList)
                {
                    bool isMapped = false;
                    if (!mappedCatalogsCache.TryGetValue(parentCatalogSitecoreId, out isMapped))
                    {
                        var parentCatalogPathIdList = this.Repository.GetPathIdsForSitecoreId(parentCatalogSitecoreId);
                        isMapped = (parentCatalogPathIdList != null && parentCatalogPathIdList.Count > 0);
                        mappedCatalogsCache[parentCatalogSitecoreId] = isMapped;
                    }

                    if (isMapped)
                    {
                        var pathIdList = this.Repository.GetPathIdsForEntityId(entity.Id);
                        foreach (var pathId in pathIdList)
                        {
                            var indexableItem = this.TryGetItem(entity, pathId);
                            if (indexableItem != null)
                            {
                                indexableList.Add(indexableItem);
                            }
                        }
                    }
                }
            }

            return indexableList;
        }

        protected override IEnumerable<CommerceCatalogIndexableItem> GetItemsToIndex()
        {
            //// called when you rebuild the index from the ui
            return Enumerable.Empty<CommerceCatalogIndexableItem>();
        }

        protected override CommerceCatalogIndexableItem GetIndexable(IIndexableUniqueId indexableUniqueId)
        {
            return this.GetItem(indexableUniqueId);
        }

        protected override CommerceCatalogIndexableItem GetIndexableAndCheckDeletes(IIndexableUniqueId indexableUniqueId)
        {
            //// called by strategy
            return this.GetItem(indexableUniqueId);
        }

        protected override IEnumerable<IIndexableUniqueId> GetIndexablesToUpdateOnDelete(IIndexableUniqueId indexableUniqueId)
        {
            return Enumerable.Empty<IIndexableUniqueId>();
        }

        protected override bool IndexUpdateNeedDelete(CommerceCatalogIndexableItem indexable)
        {
            //// called by strategy
            return true;
        }

        protected virtual CommerceCatalogIndexableItem TryGetItem(TEntity catalogEntity, string pathId)
        {
            Assert.ArgumentNotNull(catalogEntity, nameof(catalogEntity));

            Database db = Factory.GetDatabase(this.Database);
            var indexableId = ID.Parse(pathId);
            var sitecoreItem = db.GetItem(indexableId);
            if (sitecoreItem != null && sitecoreItem.Paths.IsFullyQualified)
            {
                var indexableItem = new CommerceCatalogIndexableItem(sitecoreItem, this.GetCatalogEntityLanguages(sitecoreItem));
                return indexableItem;
            }

            return null;
        }

        #region initial code
        protected override bool IsExcludedFromIndex(CommerceCatalogIndexableItem indexable, bool checkLocation = false)
        {
            bool isExcluded = base.IsExcludedFromIndex(indexable, checkLocation);
            if (!isExcluded)
            {
                using (new SecurityDisabler())
                {
                    var db = Factory.GetDatabase(this.Database);
                    Item sitecoreItem = db.GetItem(ID.Parse(indexable.Id));
                    if (sitecoreItem != null)
                    {
                        isExcluded = !IndexableTemplateIds.Contains(sitecoreItem.TemplateID);
                    }
                }
            }

            return isExcluded;
        }
        #endregion

        #region modified part of the code - added DescendsFromOrEquals method

        //protected override bool IsExcludedFromIndex(CommerceCatalogIndexableItem indexable, bool checkLocation = false)
        //{
        //    bool flag = base.IsExcludedFromIndex(indexable, checkLocation);
        //    if (!flag)
        //    {
        //        SecurityDisabler val = new SecurityDisabler();
        //        try
        //        {
        //            Item item = Factory.GetDatabase(Database).GetItem(ID.Parse((object)indexable.Id));
        //            if (item != null)
        //            {
        //                foreach (ID indexableTemplateId in IndexableTemplateIds)
        //                {
        //                    if (TemplateManager.GetTemplate(item).DescendsFromOrEquals(indexableTemplateId))
        //                    {
        //                        return false;
        //                    }
        //                }
        //                return true;
        //            }
        //            return flag;
        //        }
        //        finally
        //        {
        //            ((IDisposable)val)?.Dispose();
        //        }
        //    }
        //    return flag;
        //}

        #endregion



        protected override bool IsExcludedFromIndex(IIndexableUniqueId indexableUniqueId)
        {
            return this.IsExcludedFromIndex(indexableUniqueId, false);
        }

        #region initial code
        protected override bool IsExcludedFromIndex(IIndexableUniqueId indexableUniqueId, bool checkLocation)
        {
            bool isExcluded = base.IsExcludedFromIndex(indexableUniqueId, checkLocation);
            if (!isExcluded)
            {
                var sitecoreIndexableId = indexableUniqueId as SitecoreItemUniqueId;
                if (sitecoreIndexableId != null)
                {
                    var sitecoreItem = Data.Database.GetItem(sitecoreIndexableId);
                    if (sitecoreItem != null)
                    {
                        isExcluded = !IndexableTemplateIds.Contains(sitecoreItem.TemplateID);
                    }
                }
                else
                {
                    var commerceIndexableItem = indexableUniqueId.Value as CommerceCatalogIndexableItem;
                    if (commerceIndexableItem != null)
                    {
                        isExcluded = this.IsExcludedFromIndex(commerceIndexableItem, false);
                    }
                }
            }

            return isExcluded;
        }
        #endregion

        #region modified part of the code - added DescendsFromOrEquals method

        //protected override bool IsExcludedFromIndex(IIndexableUniqueId indexableUniqueId, bool checkLocation)
        //{
        //    bool flag = base.IsExcludedFromIndex(indexableUniqueId, checkLocation);
        //    if (!flag)
        //    {
        //        SitecoreItemUniqueId val = indexableUniqueId as SitecoreItemUniqueId;
        //        if (val != null)
        //        {
        //            Item item = Data.Database.GetItem(val);
        //            if (item != null)
        //            {
        //                foreach (ID indexableTemplateId in IndexableTemplateIds)
        //                {
        //                    if (TemplateManager.GetTemplate(item).DescendsFromOrEquals(indexableTemplateId))
        //                    {
        //                        return false;
        //                    }
        //                }
        //                flag = true;
        //            }
        //        }
        //        else
        //        {
        //            CommerceCatalogIndexableItem commerceCatalogIndexableItem = indexableUniqueId.Value as CommerceCatalogIndexableItem;
        //            if (commerceCatalogIndexableItem != null)
        //            {
        //                flag = this.IsExcludedFromIndex(commerceCatalogIndexableItem, false);
        //            }
        //        }
        //    }
        //    return flag;
        //}

        #endregion


        public override bool IsExcludedFromIndex(IIndexable indexable)
        {
            ItemUri itemUri = indexable.UniqueId as SitecoreItemUniqueId;
            return itemUri != null && !itemUri.DatabaseName.Equals(this.Database, StringComparison.OrdinalIgnoreCase);
        }

        public override void RefreshFromRoot(IProviderUpdateContext context, IIndexable indexableStartingPoint, IndexingOptions indexingOptions,
            CancellationToken cancellationToken)
        {
            Assert.ArgumentNotNull(context, "context");

            if (base.ShouldStartIndexing(indexingOptions))
            {
                this.Update(context, cancellationToken);
            }
        }

        protected virtual CommerceCatalogIndexableItem GetItem(IIndexableUniqueId indexableUniqueId)
        {
            Assert.ArgumentNotNull(indexableUniqueId, nameof(indexableUniqueId));

            if (this.IsExcludedFromIndex(indexableUniqueId))
            {
                return null;
            }

            var commerceIndexableItem = indexableUniqueId.Value as CommerceCatalogIndexableItem;
            if (commerceIndexableItem != null)
            {
                return commerceIndexableItem;
            }

            var sitecoreIndexableId = indexableUniqueId as SitecoreItemUniqueId;
            if (sitecoreIndexableId == null)
            {
                return null;
            }

            using (new SecurityDisabler())
            {
                var sitecoreItem = Data.Database.GetItem(sitecoreIndexableId);
                if (sitecoreItem != null)
                {
                    return new CommerceCatalogIndexableItem(sitecoreItem, this.GetCatalogEntityLanguages(sitecoreItem));
                }
                else
                {
                    this.LogWarn(null, "Could not find Sitecore item with URI '{0}', this item will not be indexed.", indexableUniqueId);
                    return null;
                }
            }
        }

        protected abstract ManagedList GetCatalogEntitiesToIndex(string environment, string listName, int itemsToSkip, int itemsToTake);

        protected virtual IEnumerable<Globalization.Language> GetCatalogEntityLanguages(Item item)
        {
            Assert.ArgumentNotNull(item, nameof(item));
            return item.Languages
                .Where(x => ItemManager.GetVersions(item, x).Count > 0)
                .ToList();
        }

        protected virtual void AddItemsToIndex(IProviderUpdateContext context, IEnumerable<CommerceCatalogIndexableItem> itemsToIndex)
        {
            Assert.ArgumentNotNull(context, nameof(context));
            Assert.ArgumentNotNull(itemsToIndex, nameof(itemsToIndex));

            if (context.IsParallel)
            {
                try
                {
                    var exceptions = new ConcurrentQueue<Exception>();
                    Parallel.ForEach(
                        itemsToIndex,
                        this.AddCrawlState.ParallelOptions,
                        (indexable, loopState) =>
                        {
                            this.AddCrawlState.WaitUntilUnPaused();
                            if (this.AddCrawlState.IsCancelled)
                            {
                                return;
                            }

                            if (loopState.ShouldExitCurrentIteration)
                            {
                                return;
                            }

                            try
                            {
                                if (exceptions.Count != 0)
                                {
                                    return;
                                }

                                this.DoAdd(context, indexable);

                                lock (this.AddCrawlState)
                                {
                                    ++this.AddCrawlState.CrawlCount;
                                    if (this.AddCrawlState.CrawlCount % 1000L != 0L)
                                    {
                                        return;
                                    }

                                    this.LogInfo("Added {0} items", this.AddCrawlState.CrawlCount);
                                }
                            }
                            catch (Exception ex)
                            {
                                this.LogWarn(ex, "Add failed - {0}", indexable.UniqueId);
                                if (!this.StopOnError)
                                {
                                    return;
                                }

                                exceptions.Enqueue(ex);
                            }
                        });

                    if (this.StopOnError && exceptions.Count > 0)
                    {
                        throw new AggregateException(exceptions);
                    }
                }
                catch (OperationCanceledException)
                {
                }
            }
            else
            {
                foreach (var indexable in itemsToIndex)
                {
                    this.AddCrawlState.WaitUntilUnPaused();
                    if (this.AddCrawlState.IsCancelled)
                    {
                        break;
                    }

                    if (this.AddCrawlState.CancellationToken.IsCancellationRequested)
                    {
                        break;
                    }

                    try
                    {
                        this.DoAdd(context, indexable);
                        ++this.AddCrawlState.CrawlCount;
                        if (this.AddCrawlState.CrawlCount % 1000L == 0L)
                        {
                            this.LogInfo("Added {0} items", this.AddCrawlState.CrawlCount);
                        }
                    }
                    catch (Exception ex)
                    {
                        this.LogWarn(ex, "Add failed for item with ID {0}", indexable.UniqueId);
                        if (this.StopOnError)
                        {
                            throw;
                        }
                    }
                }
            }
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Maintainability", "CA1502:AvoidExcessiveComplexity", Justification = "Content reviewed.")]
        protected virtual void UpdateItemsInIndex(IProviderUpdateContext context, IEnumerable<CommerceCatalogIndexableItem> itemsToIndex)
        {
            Assert.ArgumentNotNull(context, "context");
            Assert.ArgumentNotNull(itemsToIndex, "itemsToIndex");

            if (context.IsParallel)
            {
                if (itemsToIndex.Any())
                {
                    try
                    {
                        var exceptions = new ConcurrentQueue<Exception>();
                        Parallel.ForEach(
                            itemsToIndex.Where(i => i != null),
                            this.UpdateCrawlState.ParallelOptions,
                            (indexable, loopState) =>
                            {
                                this.UpdateCrawlState.WaitUntilUnPaused();
                                if (this.UpdateCrawlState.IsCancelled)
                                {
                                    return;
                                }

                                if (loopState.ShouldExitCurrentIteration)
                                {
                                    return;
                                }

                                try
                                {
                                    if (exceptions.Count != 0)
                                    {
                                        return;
                                    }

                                    this.DoUpdate(context, indexable);

                                    lock (this.UpdateCrawlState)
                                    {
                                        ++this.UpdateCrawlState.CrawlCount;
                                        if (this.UpdateCrawlState.CrawlCount % 1000L != 0L)
                                        {
                                            return;
                                        }

                                        this.LogInfo("Updated {0} items", this.UpdateCrawlState.CrawlCount);
                                    }
                                }
                                catch (Exception ex)
                                {
                                    this.LogWarn(ex, "Update failed for item with ID {0}", indexable.UniqueId);
                                    if (!this.StopOnError)
                                    {
                                        return;
                                    }

                                    exceptions.Enqueue(ex);
                                }
                            });

                        if (!this.StopOnError || exceptions.Count <= 0)
                        {
                            return;
                        }

                        throw new AggregateException(exceptions);
                    }
                    catch (OperationCanceledException)
                    {
                        return;
                    }
                    catch (Exception ex)
                    {
                        this.LogError(ex, "Unexpected error occured in {0}", nameof(this.UpdateItemsInIndex));
                        return;
                    }
                }
            }

            foreach (var indexable in itemsToIndex.Where(i => i != null))
            {
                this.UpdateCrawlState.WaitUntilUnPaused();
                if (this.UpdateCrawlState.IsCancelled)
                {
                    break;
                }

                if (this.UpdateCrawlState.CancellationToken.IsCancellationRequested)
                {
                    break;
                }

                try
                {
                    this.DoUpdate(context, indexable);
                    ++this.UpdateCrawlState.CrawlCount;
                    if (this.UpdateCrawlState.CrawlCount % 1000L == 0L)
                    {
                        this.LogInfo("Updated {0} items", this.UpdateCrawlState.CrawlCount);
                    }
                }
                catch (Exception ex)
                {
                    this.LogWarn(ex, "Update failed for item with ID {0}", indexable.UniqueId);
                    if (this.StopOnError)
                    {
                        throw;
                    }
                }
            }
        }

        protected override void DoAdd(IProviderUpdateContext context, CommerceCatalogIndexableItem indexable)
        {
            Assert.ArgumentNotNull(context, "context");
            Assert.ArgumentNotNull(indexable, "indexable");
            Event.RaiseEvent("indexing:adding", new object[] { context.Index.Name, indexable.UniqueId });

            if (this.IsExcludedFromIndex(indexable, false))
            {
                return;
            }

            Database db = Factory.GetDatabase(this.Database);

            using (new SecurityDisabler())
            {
                if (indexable.Languages.Any())
                {
                    foreach (Globalization.Language language in indexable.Languages)
                    {
                        var sitecoreItem = db.GetItem(indexable.SitecoreId, language);
                        var indexableItem = new SitecoreIndexableItem(sitecoreItem);

                        IIndexableBuiltinFields indexableBuiltinFields = indexableItem;
                        indexableBuiltinFields.IsLatestVersion = true;

                        this.Operations.Add(indexableItem, context, this.index.Configuration);
                    }
                }
                else
                {
                    var sitecoreItem = db.GetItem(indexable.SitecoreId);
                    var indexableItem = new SitecoreIndexableItem(sitecoreItem);

                    IIndexableBuiltinFields indexableBuiltinFields = indexableItem;
                    indexableBuiltinFields.IsLatestVersion = true;

                    this.Operations.Add(indexableItem, context, this.index.Configuration);
                }
            }

            Event.RaiseEvent("indexing:added", new object[] { context.Index.Name, indexable.UniqueId });
        }

        protected override void DoUpdate(IProviderUpdateContext context, CommerceCatalogIndexableItem indexable, IndexEntryOperationContext operationContext)
        {
            Assert.ArgumentNotNull(context, nameof(context));
            Assert.ArgumentNotNull(indexable, nameof(indexable));

            if (!this.IsExcludedFromIndex(indexable, false))
            {
                if (this.IndexUpdateNeedDelete(indexable))
                {
                    var deleteParams = new object[] { this.index.Name, indexable.UniqueId, indexable.AbsolutePath };
                    Event.RaiseEvent("indexing:deleteitem", deleteParams);
                    this.Operations.Delete(indexable, context);
                }

                Database targetDatabase = Factory.GetDatabase(this.Database);
                object[] updatingParams = new object[] { this.index.Name, indexable.UniqueId, indexable.AbsolutePath };
                Event.RaiseEvent("indexing:updatingitem", updatingParams);
                if (!this.IsExcludedFromIndex(indexable, true))
                {
                    var catalogLanguages = this.Repository.GetLanguages();
                    foreach (var language in catalogLanguages)
                    {
                        var item = targetDatabase.GetItem(indexable.SitecoreId, Globalization.Language.Parse(language));
                        if (item != null)
                        {
                            var indexableItem = new SitecoreIndexableItem(item);

                            IIndexableBuiltinFields indexableBuiltinFields = indexableItem;
                            indexableBuiltinFields.IsLatestVersion = true;

                            this.Operations.Update(indexableItem, context, this.index.Configuration);
                        }
                    }

                    object[] updatedParams = new object[] { this.index.Name, indexable.UniqueId, indexable.AbsolutePath };
                    Event.RaiseEvent("indexing:updateditem", updatedParams);
                }

                if (this.DocumentOptions.ProcessDependencies)
                {
                    object[] updateDependentsParams = new object[] { this.index.Name, indexable.UniqueId, indexable.AbsolutePath };
                    Event.RaiseEvent("indexing:updatedependents", updateDependentsParams);
                    this.UpdateDependents(context, indexable);
                }
            }
        }

        private string FormatMessage(string format, params object[] args)
        {
            var prefix = string.Format(
                CultureInfo.InvariantCulture,
                "[Index={0}] : {1} ",
                this.Index.Name,
                this.GetType().Name);

            var message = format;
            if (args != null || args.Length > 0)
            {
                message = string.Format(CultureInfo.InvariantCulture, format, args);
            }

            return string.Concat(prefix, message);
        }

        protected virtual void LogInfo(string format, params object[] args)
        {
            CrawlingLog.Log.Info(this.FormatMessage(format, args));
        }

        protected virtual void LogWarn(Exception ex, string format, params object[] args)
        {
            CrawlingLog.Log.Warn(this.FormatMessage(format, args), ex);
        }

        protected virtual void LogError(Exception ex, string format, params object[] args)
        {
            CrawlingLog.Log.Error(this.FormatMessage(format, args), ex);
        }
    }
}
