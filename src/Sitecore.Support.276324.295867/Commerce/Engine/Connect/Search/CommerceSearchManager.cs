using Sitecore;
using Sitecore.Commerce.Engine.Connect;
using Sitecore.Commerce.Engine.Connect.Interfaces;
using Sitecore.Commerce.Engine.Connect.Search;
using Sitecore.Commerce.Engine.Connect.Search.Models;
using Sitecore.ContentSearch;
using Sitecore.ContentSearch.Linq;
using Sitecore.ContentSearch.SearchTypes;
using Sitecore.Data.Items;
using Sitecore.Data.Managers;
using Sitecore.Diagnostics;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Sitecore.Support.Commerce.Engine.Connect.Search
{
    public class CommerceSearchManager : ICommerceSearchManager
    {
        private static object _associationLock = new object();

        public virtual bool IsItemCatalog(Item item)
        {
            return item.TemplateID == CommerceConstants.KnownTemplateIds.CommerceCatalogTemplate;
        }

        public virtual IQueryable<T> AddSearchOptionsToQuery<T>(IQueryable<T> query, CommerceSearchOptions searchOptions)
            where T : ISearchResult
        {
            Assert.ArgumentNotNull(query, "query");

            if (searchOptions == null)
            {
                searchOptions = new CommerceSearchOptions();
            }

            if (searchOptions.HasPaging)
            {
                query = query.Page(searchOptions.StartPageIndex, searchOptions.NumberOfItemsToReturn);
            }

            if (searchOptions.HasFacets)
            {
                searchOptions.FacetFields.ToList().ForEach(facetField => query = query.FacetOn(f => f[facetField.Name]));

                var facetsWithValues = searchOptions.FacetFields.Where(f => f.Values != null && f.Values.Count > 0);

                foreach (var facetValue in facetsWithValues)
                {
                    foreach (var value in facetValue.Values)
                    {
                        var compareValue = value as string;

                        query = query.Where(f => f[facetValue.Name] == compareValue);
                    }
                }
            }

            query = this.ApplySorting(query, searchOptions);

            return query;
        }

        public virtual IQueryable<SitecoreUISearchResultItem> AddSearchOptionsToQuery(IQueryable<SitecoreUISearchResultItem> query, CommerceSearchOptions searchOptions)
        {
            return this.AddSearchOptionsToQuery<SitecoreUISearchResultItem>(query, searchOptions);
        }

        public virtual IEnumerable<CommerceQueryFacet> GetFacetFieldsForItem(Item item)
        {
            Assert.IsNotNull(item, "item");

            var returnList = new List<CommerceQueryFacet>();

            if (item.Fields[CommerceConstants.KnownFieldIds.CategorySearchFacets] != null)
            {
                var itemDb = item.Database;

                var mlf = (Data.Fields.MultilistField)item.Fields[CommerceConstants.KnownFieldIds.CategorySearchFacets];

                foreach (var mlfItem in mlf.TargetIDs)
                {
                    var facetItem = itemDb.GetItem(mlfItem);
                    var commerceFacetItem = new CommerceFacetItem(facetItem);

                    if (commerceFacetItem.IsFacet && commerceFacetItem.IsEnabled && !string.IsNullOrWhiteSpace(commerceFacetItem.FieldName))
                    {
                        returnList.Add(new CommerceQueryFacet() { Name = commerceFacetItem.FieldName, DisplayName = commerceFacetItem.DisplayName });
                    }
                }
            }

            return returnList;
        }

        public int GetItemsPerPageForItem(Item item)
        {
            Assert.IsNotNull(item, "item");
            int itemsPerPage = 0;

            if (item.Fields[CommerceConstants.KnownFieldIds.ItemsPerPage] != null)
            {
                var pageField = (Data.Fields.TextField)item.Fields[CommerceConstants.KnownFieldIds.ItemsPerPage];
                var parseInt = int.TryParse(pageField.Value, out itemsPerPage);
                if (parseInt)
                {
                    return itemsPerPage;
                }
                else
                {
                    return 0;
                }
            }

            return itemsPerPage;
        }

        public virtual IEnumerable<CommerceQuerySort> GetSortFieldsForItem(Item item)
        {
            Assert.IsNotNull(item, "item");

            var returnList = new List<CommerceQuerySort>();

            if (item.Fields[CommerceConstants.KnownFieldIds.SortFields] != null)
            {
                var itemDb = item.Database;

                var mlf = (Data.Fields.MultilistField)item.Fields[CommerceConstants.KnownFieldIds.SortFields];

                foreach (var mlfItem in mlf.TargetIDs)
                {
                    var facetItem = itemDb.GetItem(mlfItem);
                    var commerceFacetItem = new CommerceFacetItem(facetItem);

                    if (commerceFacetItem.IsEnabled && !string.IsNullOrWhiteSpace(commerceFacetItem.FieldName))
                    {
                        returnList.Add(new CommerceQuerySort() { Name = commerceFacetItem.FieldName, DisplayName = commerceFacetItem.DisplayName });
                    }
                }
            }

            return returnList;
        }

        public virtual ISearchIndex GetIndex()
        {
            return this.GetIndex(null);
        }

        public virtual ISearchIndex GetIndex(string catalogName)
        {
            return this.GetIndex(Context.Database.Name, catalogName);
        }

        public virtual ISearchIndex GetIndex(string databaseName, string catalogName)
        {
            Assert.IsNotNullOrEmpty(databaseName, "databaseName");

            var resolver = CommerceTypeLoader.CreateInstance<IIndexResolver>();
            var indexName = resolver.GetIndexName(Data.Database.GetDatabase(databaseName), catalogName);
            var index = ContentSearchManager.GetIndex(indexName);

            return index;
        }


        protected virtual IQueryable<T> ApplySorting<T>(IQueryable<T> query, CommerceSearchOptions searchOptions)
            where T : ISearchResult
        {
            if (!string.IsNullOrEmpty(searchOptions.SortField))
            {
                var sorters = CommerceTypeLoader.CreateInstances<ISortProvider>();

                if (sorters != null && sorters.Count() > 0)
                {
                    foreach (var sorter in sorters)
                    {
                        if (sorter.CanHandle(query, searchOptions))
                        {
                            query = sorter.ApplySorting(query, searchOptions);
                            break;
                        }
                    }
                }
            }

            return query;
        }

        //#region modified part of the code - added DescendsFromOrEquals method
        //public bool IsItemCategory(Item item)
        //{
        //    return TemplateManager.GetTemplate(item).DescendsFromOrEquals(CommerceConstants.KnownTemplateIds.CommerceCategoryTemplate);
        //}

        //public bool IsItemProduct(Item item)
        //{
        //    return TemplateManager.GetTemplate(item).DescendsFromOrEquals(CommerceConstants.KnownTemplateIds.CommerceProductTemplate);
        //}

        //public bool IsItemVariant(Item item)
        //{
        //    return TemplateManager.GetTemplate(item).DescendsFromOrEquals(CommerceConstants.KnownTemplateIds.CommerceProductVariantTemplate);
        //}

        //public bool IsItemNavigation(Item item)
        //{
        //    return TemplateManager.GetTemplate(item).DescendsFromOrEquals(CommerceConstants.KnownTemplateIds.CommerceNavigationItemTemplate);
        //}

        //#endregion

        #region initial code
        public bool IsItemCategory(Item item)
        {
            return item.TemplateID == CommerceConstants.KnownTemplateIds.CommerceCategoryTemplate;
        }

        public bool IsItemProduct(Item item)
        {
            return item.TemplateID == CommerceConstants.KnownTemplateIds.CommerceProductTemplate;
        }

        public bool IsItemVariant(Item item)
        {
            return item.TemplateID == CommerceConstants.KnownTemplateIds.CommerceProductVariantTemplate;
        }

        public bool IsItemNavigation(Item item)
        {
            return item.TemplateID == CommerceConstants.KnownTemplateIds.CommerceNavigationItemTemplate;
        }

        #endregion

    }
}
