/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree and available online at
 *
 * http://www.dspace.org/license/
 */
package org.dspace.content;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.dspace.app.util.AuthorizeUtil;
import org.dspace.authorize.AuthorizeConfiguration;
import org.dspace.authorize.AuthorizeException;
import org.dspace.authorize.ResourcePolicy;
import org.dspace.authorize.service.AuthorizeService;
import org.dspace.authorize.service.ResourcePolicyService;
import org.dspace.content.authority.Choices;
import org.dspace.content.dao.ItemDAO;
import org.dspace.content.service.*;
import org.dspace.core.Constants;
import org.dspace.core.Context;
import org.dspace.core.LogManager;
import org.dspace.eperson.EPerson;
import org.dspace.eperson.Group;
import org.dspace.event.Event;
import org.dspace.harvest.HarvestedItem;
import org.dspace.harvest.service.HarvestedItemService;
import org.dspace.identifier.IdentifierException;
import org.dspace.identifier.service.IdentifierService;
import org.dspace.versioning.service.VersioningService;
import org.springframework.beans.factory.annotation.Autowired;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.*;

/**
 * Service implementation for the Item object.
 * This class is responsible for all business logic calls for the Item object and is autowired by spring.
 * This class should never be accessed directly.
 *
 * @author kevinvandevelde at atmire.com
 */
public class ItemServiceImpl extends DSpaceObjectServiceImpl<Item> implements ItemService {

    /**
     * log4j category
     */
    private static final Logger log = Logger.getLogger(Item.class);

    @Autowired(required = true)
    protected ItemDAO itemDAO;

    @Autowired(required = true)
    protected CommunityService communityService;

    @Autowired(required = true)
    protected AuthorizeService authorizeService;

    @Autowired(required = true)
    protected BundleService bundleService;

    @Autowired(required = true)
    protected BitstreamFormatService bitstreamFormatService;

    @Autowired(required = true)
    protected MetadataSchemaService metadataSchemaService;

    @Autowired(required = true)
    protected BitstreamService bitstreamService;

    @Autowired(required = true)
    protected InstallItemService installItemService;

    @Autowired(required = true)
    protected ResourcePolicyService resourcePolicyService;

    @Autowired(required = true)
    protected CollectionService collectionService;

    @Autowired(required = true)
    protected IdentifierService identifierService;

    @Autowired(required = true)
    protected VersioningService versioningService;

    @Autowired(required = true)
    protected HarvestedItemService harvestedItemService;

    protected ItemServiceImpl() {
        super();
    }

    @Override
    public Thumbnail getThumbnail(Context context, Item item, boolean requireOriginal) throws SQLException {
        Bitstream thumbBitstream;
        List<Bundle> originalBundles = getBundles(item, "ORIGINAL");
        Bitstream primaryBitstream = null;
        if (CollectionUtils.isNotEmpty(originalBundles)) {
            primaryBitstream = originalBundles.get(0).getPrimaryBitstream();
        }
        if (primaryBitstream != null) {
            if (primaryBitstream.getFormat(context).getMIMEType().equals("text/html")) {
                return null;
            }
            thumbBitstream = bitstreamService.getBitstreamByName(item, "THUMBNAIL", primaryBitstream.getName() + ".jpg");
        } else {
            if (requireOriginal) {
                primaryBitstream = bitstreamService.getFirstBitstream(item, "ORIGINAL");
            }
            thumbBitstream = bitstreamService.getFirstBitstream(item, "THUMBNAIL");
        }
        if (thumbBitstream != null) {
            return new Thumbnail(thumbBitstream, primaryBitstream);
        }
        return null;
    }

    @Override
    public Item find(Context context, UUID id) throws SQLException {
        Item item = itemDAO.findByID(context, Item.class, id);
        if (item == null) {
            if (log.isDebugEnabled()) {
                log.debug(LogManager.getHeader(context, "find_item", "not_found,item_id=" + id));
            }
            return null;
        }
        if (log.isDebugEnabled()) {
            log.debug(LogManager.getHeader(context, "find_item", "item_id=" + id));
        }
        return item;
    }

    @Override
    public Item create(Context context, WorkspaceItem workspaceItem) throws SQLException, AuthorizeException {
        if (workspaceItem.getItem() != null) {
            throw new IllegalArgumentException("Attempting to create an item for a workspace item that already contains an item");
        }
        Item item = createItem(context);
        workspaceItem.setItem(item);
        log.info(LogManager.getHeader(context, "create_item", "item_id=" + item.getID()));
        return item;
    }

    @Override
    public Item createTemplateItem(Context context, Collection collection) throws SQLException, AuthorizeException {
        if (collection == null || collection.getTemplateItem() != null) {
            throw new IllegalArgumentException("Collection is null or already contains template item.");
        }
        AuthorizeUtil.authorizeManageTemplateItem(context, collection);
        if (collection.getTemplateItem() == null) {
            Item template = createItem(context);
            collection.setTemplateItem(template);
            template.setTemplateItemOf(collection);
            log.info(LogManager.getHeader(context, "create_template_item", "collection_id=" + collection.getID() + ",template_item_id=" + template.getID()));
            return template;
        } else {
            return collection.getTemplateItem();
        }
    }

    @Override
    public Iterator<Item> findAll(Context context) throws SQLException {
        return itemDAO.findAll(context, true);
    }

    @Override
    public Iterator<Item> findAllUnfiltered(Context context) throws SQLException {
        return itemDAO.findAll(context, true, true);
    }

    @Override
    public Iterator<Item> findBySubmitter(Context context, EPerson eperson) throws SQLException {
        return itemDAO.findBySubmitter(context, eperson);
    }

    @Override
    public Iterator<Item> findBySubmitterDateSorted(Context context, EPerson eperson, Integer limit) throws SQLException {
        MetadataField metadataField = metadataFieldService.findByElement(context, MetadataSchema.DC_SCHEMA, "date", "accessioned");
        return itemDAO.findBySubmitter(context, eperson, metadataField, limit);
    }

    @Override
    public Iterator<Item> findByCollection(Context context, Collection collection) throws SQLException {
        return findByCollection(context, collection, null, null);
    }

    @Override
    public Iterator<Item> findByCollection(Context context, Collection collection, Integer limit, Integer offset) throws SQLException {
        return itemDAO.findArchivedByCollection(context, collection, limit, offset);
    }

    @Override
    public Iterator<Item> findAllByCollection(Context context, Collection collection) throws SQLException {
        return itemDAO.findAllByCollection(context, collection);
    }

    @Override
    public Iterator<Item> findInArchiveOrWithdrawnDiscoverableModifiedSince(Context context, Date since) throws SQLException {
        return itemDAO.findAll(context, true, true, true, since);
    }

    @Override
    public void updateLastModified(Context context, Item item) throws SQLException, AuthorizeException {
        item.setLastModified(new Date());
        update(context, item);
        context.addEvent(new Event(Event.MODIFY, Constants.ITEM, item.getID(), null, getIdentifiers(context, item)));
    }

    @Override
    public boolean isIn(Item item, Collection collection) throws SQLException {
        List<Collection> collections = item.getCollections();
        return collections != null && collections.contains(collection);
    }

    @Override
    public List<Community> getCommunities(Context context, Item item) throws SQLException {
        List<Community> result = new ArrayList<>();
        List<Collection> collections = item.getCollections();
        for (Collection collection : collections) {
            List<Community> owningCommunities = collection.getCommunities();
            for (Community community : owningCommunities) {
                result.add(community);
                result.addAll(communityService.getAllParents(context, community));
            }
        }
        return result;
    }

    @Override
    public List<Bundle> getBundles(Item item, String name) throws SQLException {
        List<Bundle> matchingBundles = new ArrayList<>();
        List<Bundle> bunds = item.getBundles();
        for (Bundle bund : bunds) {
            if (name.equals(bund.getName())) {
                matchingBundles.add(bund);
            }
        }
        return matchingBundles;
    }

    @Override
    public void addBundle(Context context, Item item, Bundle bundle) throws SQLException, AuthorizeException {
        authorizeService.authorizeAction(context, item, Constants.ADD);
        log.info(LogManager.getHeader(context, "add_bundle", "item_id=" + item.getID() + ",bundle_id=" + bundle.getID()));
        if (item.getBundles().contains(bundle)) {
            return;
        }
        authorizeService.inheritPolicies(context, item, bundle);
        item.addBundle(bundle);
        bundle.addItem(item);
        context.addEvent(new Event(Event.ADD, Constants.ITEM, item.getID(), Constants.BUNDLE, bundle.getID(), bundle.getName(), getIdentifiers(context, item)));
    }

    @Override
    public void removeBundle(Context context, Item item, Bundle bundle) throws SQLException, AuthorizeException, IOException {
        authorizeService.authorizeAction(context, item, Constants.REMOVE);
        log.info(LogManager.getHeader(context, "remove_bundle", "item_id=" + item.getID() + ",bundle_id=" + bundle.getID()));
        item.removeBundle(bundle);
        bundle.removeItem(item);
        context.addEvent(new Event(Event.REMOVE, Constants.ITEM, item.getID(), Constants.BUNDLE, bundle.getID(), bundle.getName(), getIdentifiers(context, item)));
        if (CollectionUtils.isEmpty(bundle.getItems())) {
            bundleService.delete(context, bundle);
        }
    }

    @Override
    public Bitstream createSingleBitstream(Context context, InputStream is, Item item, String name) throws AuthorizeException, IOException, SQLException {
        Bundle bnd = bundleService.create(context, item, name);
        Bitstream bitstream = bitstreamService.create(context, bnd, is);
        addBundle(context, item, bnd);
        return bitstream;
    }

    @Override
    public Bitstream createSingleBitstream(Context context, InputStream is, Item item) throws AuthorizeException, IOException, SQLException {
        return createSingleBitstream(context, is, item, "ORIGINAL");
    }

    @Override
    public List<Bitstream> getNonInternalBitstreams(Context context, Item item) throws SQLException {
        List<Bitstream> bitstreamList = new ArrayList<>();
        List<Bundle> bunds = item.getBundles();
        for (Bundle bund : bunds) {
            List<Bitstream> bitstreams = bund.getBitstreams();
            for (Bitstream bitstream : bitstreams) {
                if (!bitstream.getFormat(context).isInternal()) {
                    bitstreamList.add(bitstream);
                }
            }
        }
        return bitstreamList;
    }

    protected Item createItem(Context context) throws SQLException, AuthorizeException {
        Item item = itemDAO.create(context, new Item());
        item.setDiscoverable(true);
        context.turnOffAuthorisationSystem();
        update(context, item);
        context.restoreAuthSystemState();
        context.addEvent(new Event(Event.CREATE, Constants.ITEM, item.getID(), null, getIdentifiers(context, item)));
        log.info(LogManager.getHeader(context, "create_item", "item_id=" + item.getID()));
        return item;
    }

    @Override
    public void removeDSpaceLicense(Context context, Item item) throws SQLException, AuthorizeException, IOException {
        List<Bundle> bunds = getBundles(item, "LICENSE");
        for (Bundle bund : bunds) {
            removeBundle(context, item, bund);
        }
    }

    @Override
    public void removeLicenses(Context context, Item item) throws SQLException, AuthorizeException, IOException {
        BitstreamFormat bf = bitstreamFormatService.findByShortDescription(context, "License");
        int licensetype = bf.getID();
        List<Bundle> bunds = item.getBundles();
        for (Bundle bund : bunds) {
            boolean removethisbundle = false;
            List<Bitstream> bits = bund.getBitstreams();
            for (Bitstream bit : bits) {
                BitstreamFormat bft = bit.getFormat(context);
                if (bft.getID() == licensetype) {
                    removethisbundle = true;
                }
            }
            if (removethisbundle) {
                removeBundle(context, item, bund);
            }
        }
    }

    @Override
    public void update(Context context, Item item) throws SQLException, AuthorizeException {
        if (!canEdit(context, item)) {
            authorizeService.authorizeAction(context, item, Constants.WRITE);
        }
        log.info(LogManager.getHeader(context, "update_item", "item_id=" + item.getID()));
        super.update(context, item);
        int sequence = 0;
        List<Bundle> bunds = item.getBundles();
        for (Bundle bund : bunds) {
            List<Bitstream> streams = bund.getBitstreams();
            for (Bitstream bitstream : streams) {
                if (bitstream.getSequenceID() > sequence) {
                    sequence = bitstream.getSequenceID();
                }
            }
        }
        sequence++;
        for (Bundle bund : bunds) {
            List<Bitstream> streams = bund.getBitstreams();
            for (Bitstream stream : streams) {
                if (stream.getSequenceID() < 0) {
                    stream.setSequenceID(sequence);
                    sequence++;
                    bitstreamService.update(context, stream);
                }
            }
        }
        if (item.isMetadataModified() || item.isModified()) {
            item.setLastModified(new Date());
            itemDAO.save(context, item);
            if (item.isMetadataModified()) {
                context.addEvent(new Event(Event.MODIFY_METADATA, item.getType(), item.getID(), item.getDetails(), getIdentifiers(context, item)));
            }
            context.addEvent(new Event(Event.MODIFY, Constants.ITEM, item.getID(), null, getIdentifiers(context, item)));
            item.clearModified();
            item.clearDetails();
        }
    }

    @Override
    public void withdraw(Context context, Item item) throws SQLException, AuthorizeException {
        AuthorizeUtil.authorizeWithdrawItem(context, item);
        String timestamp = DCDate.getCurrent().toString();
        EPerson e = context.getCurrentUser();
        StringBuilder prov = new StringBuilder();
        prov.append("Item withdrawn by ").append(e.getFullName()).append(" (").append(e.getEmail()).append(") on ").append(timestamp).append("\n").append("Item was in collections:\n");
        List<Collection> colls = item.getCollections();
        for (Collection coll : colls) {
            prov.append(coll.getName()).append(" (ID: ").append(coll.getID()).append(")\n");
        }
        item.setWithdrawn(true);
        item.setArchived(false);
        prov.append(installItemService.getBitstreamProvenanceMessage(context, item));
        addMetadata(context, item, MetadataSchema.DC_SCHEMA, "description", "provenance", "en", prov.toString());
        update(context, item);
        context.addEvent(new Event(Event.MODIFY, Constants.ITEM, item.getID(), "WITHDRAW", getIdentifiers(context, item)));
        authorizeService.removeAllPoliciesByDSOAndTypeNotEqualsTo(context, item, ResourcePolicy.TYPE_CUSTOM);
        log.info(LogManager.getHeader(context, "withdraw_item", "user=" + e.getEmail() + ",item_id=" + item.getID()));
    }

    @Override
    public void reinstate(Context context, Item item) throws SQLException, AuthorizeException {
        AuthorizeUtil.authorizeReinstateItem(context, item);
        String timestamp = DCDate.getCurrent().toString();
        List<Collection> colls = item.getCollections();
        EPerson e = context.getCurrentUser();
        StringBuilder prov = new StringBuilder();
        prov.append("Item reinstated by ").append(e.getFullName()).append(" (").append(e.getEmail()).append(") on ").append(timestamp).append("\n").append("Item was in collections:\n");
        for (Collection coll : colls) {
            prov.append(coll.getName()).append(" (ID: ").append(coll.getID()).append(")\n");
        }
        item.setWithdrawn(false);
        item.setArchived(true);
        prov.append(installItemService.getBitstreamProvenanceMessage(context, item));
        addMetadata(context, item, MetadataSchema.DC_SCHEMA, "description", "provenance", "en", prov.toString());
        update(context, item);
        context.addEvent(new Event(Event.MODIFY, Constants.ITEM, item.getID(), "REINSTATE", getIdentifiers(context, item)));
        if (colls.size() > 0) {
            inheritCollectionDefaultPolicies(context, item, colls.iterator().next());
        }
        log.info(LogManager.getHeader(context, "reinstate_item", "user=" + e.getEmail() + ",item_id=" + item.getID()));
    }

    @Override
    public void delete(Context context, Item item) throws SQLException, AuthorizeException, IOException {
        authorizeService.authorizeAction(context, item, Constants.DELETE);
        HarvestedItem hi = harvestedItemService.find(context, item);
        if (hi != null) {
            harvestedItemService.delete(context, hi);
        }
        rawDelete(context, item);
    }

    @Override
    public int getSupportsTypeConstant() {
        return Constants.ITEM;
    }

    protected void rawDelete(Context context, Item item) throws AuthorizeException, SQLException, IOException {
        authorizeService.authorizeAction(context, item, Constants.REMOVE);
        context.addEvent(new Event(Event.DELETE, Constants.ITEM, item.getID(), item.getHandle(), getIdentifiers(context, item)));
        log.info(LogManager.getHeader(context, "delete_item", "item_id=" + item.getID()));
        removeAllBundles(context, item);
        removeVersion(context, item);
        item.getCollections().clear();
        item.setOwningCollection(null);
        handleService.unbindHandle(context, item);
        itemDAO.delete(context, item);
    }

    @Override
    public void removeAllBundles(Context context, Item item) throws AuthorizeException, SQLException, IOException {
        Iterator<Bundle> bundles = item.getBundles().iterator();
        while (bundles.hasNext()) {
            Bundle bundle = bundles.next();
            bundles.remove();
            deleteBundle(context, item, bundle);
        }
    }

    protected void deleteBundle(Context context, Item item, Bundle b) throws AuthorizeException, SQLException, IOException {
        authorizeService.authorizeAction(context, item, Constants.REMOVE);
        bundleService.delete(context, b);
        log.info(LogManager.getHeader(context, "remove_bundle", "item_id=" + item.getID() + ",bundle_id=" + b.getID()));
        context.addEvent(new Event(Event.REMOVE, Constants.ITEM, item.getID(), Constants.BUNDLE, b.getID(), b.getName()));
    }

    protected void removeVersion(Context context, Item item) throws AuthorizeException, SQLException {
        if (versioningService.getVersion(context, item) != null) {
            versioningService.removeVersion(context, item);
        } else {
            try {
                identifierService.delete(context, item);
            } catch (IdentifierException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public boolean isOwningCollection(Item item, Collection collection) {
        Collection owningCollection = item.getOwningCollection();
        return owningCollection != null && collection.getID().equals(owningCollection.getID());
    }

    @Override
    public void replaceAllItemPolicies(Context context, Item item, List<ResourcePolicy> newpolicies) throws SQLException, AuthorizeException {
        authorizeService.removeAllPolicies(context, item);
        authorizeService.addPolicies(context, newpolicies, item);
    }

    @Override
    public void replaceAllBitstreamPolicies(Context context, Item item, List<ResourcePolicy> newpolicies) throws SQLException, AuthorizeException {
        List<Bundle> bunds = item.getBundles();
        for (Bundle mybundle : bunds) {
            bundleService.replaceAllBitstreamPolicies(context, mybundle, newpolicies);
        }
    }

    @Override
    public void removeGroupPolicies(Context context, Item item, Group group) throws SQLException, AuthorizeException {
        authorizeService.removeGroupPolicies(context, item, group);
        List<Bundle> bunds = item.getBundles();
        for (Bundle mybundle : bunds) {
            List<Bitstream> bs = mybundle.getBitstreams();
            for (Bitstream bitstream : bs) {
                authorizeService.removeGroupPolicies(context, bitstream, group);
            }
            authorizeService.removeGroupPolicies(context, mybundle, group);
        }
    }

    @Override
    public void inheritCollectionDefaultPolicies(Context context, Item item, Collection collection) throws SQLException, AuthorizeException {
        adjustItemPolicies(context, item, collection);
        adjustBundleBitstreamPolicies(context, item, collection);
        log.debug(LogManager.getHeader(context, "item_inheritCollectionDefaultPolicies", "item_id=" + item.getID()));
    }

    @Override
    public void adjustBundleBitstreamPolicies(Context context, Item item, Collection collection) throws SQLException, AuthorizeException {
        List<ResourcePolicy> defaultCollectionPolicies = authorizeService.getPoliciesActionFilter(context, collection, Constants.DEFAULT_BITSTREAM_READ);
        if (defaultCollectionPolicies.size() < 1) {
            throw new SQLException("Collection " + collection.getID() + " (" + collection.getHandle() + ")" + " has no default bitstream READ policies");
        }
        List<Bundle> bunds = item.getBundles();
        for (Bundle mybundle : bunds) {
            authorizeService.removeAllPoliciesByDSOAndType(context, mybundle, ResourcePolicy.TYPE_SUBMISSION);
            authorizeService.removeAllPoliciesByDSOAndType(context, mybundle, ResourcePolicy.TYPE_WORKFLOW);
            List<ResourcePolicy> policiesBundleToAdd = filterPoliciesToAdd(context, defaultCollectionPolicies, mybundle);
            authorizeService.addPolicies(context, policiesBundleToAdd, mybundle);
            for (Bitstream bitstream : mybundle.getBitstreams()) {
                authorizeService.removeAllPoliciesByDSOAndType(context, bitstream, ResourcePolicy.TYPE_SUBMISSION);
                authorizeService.removeAllPoliciesByDSOAndType(context, bitstream, ResourcePolicy.TYPE_WORKFLOW);
                List<ResourcePolicy> policiesBitstreamToAdd = filterPoliciesToAdd(context, defaultCollectionPolicies, bitstream);
                authorizeService.addPolicies(context, policiesBitstreamToAdd, bitstream);
            }
        }
    }

    @Override
    public void adjustItemPolicies(Context context, Item item, Collection collection) throws SQLException, AuthorizeException {
        List<ResourcePolicy> defaultCollectionPolicies = authorizeService.getPoliciesActionFilter(context, collection, Constants.DEFAULT_ITEM_READ);
        if (defaultCollectionPolicies.size() < 1) {
            throw new SQLException("Collection " + collection.getID() + " (" + collection.getHandle() + ")" + " has no default item READ policies");
        }
        try {
            context.turnOffAuthorisationSystem();
            authorizeService.removeAllPoliciesByDSOAndType(context, item, ResourcePolicy.TYPE_SUBMISSION);
            authorizeService.removeAllPoliciesByDSOAndType(context, item, ResourcePolicy.TYPE_WORKFLOW);
            List<ResourcePolicy> policiesToAdd = filterPoliciesToAdd(context, defaultCollectionPolicies, item);
            authorizeService.addPolicies(context, policiesToAdd, item);
        } finally {
            context.restoreAuthSystemState();
        }
    }

    @Override
    public void move(Context context, Item item, Collection from, Collection to) throws SQLException, AuthorizeException, IOException {
        this.move(context, item, from, to, false);
    }

    @Override
    public void move(Context context, Item item, Collection from, Collection to, boolean inheritDefaultPolicies) throws SQLException, AuthorizeException, IOException {
        if (!canEdit(context, item)) {
            authorizeService.authorizeAction(context, item, Constants.WRITE);
        }
        collectionService.addItem(context, to, item);
        collectionService.removeItem(context, from, item);
        if (isOwningCollection(item, from)) {
            log.info(LogManager.getHeader(context, "move_item", "item_id=" + item.getID() + ", from " + "collection_id=" + from.getID() + " to " + "collection_id=" + to.getID()));
            item.setOwningCollection(to);
            if (inheritDefaultPolicies) {
                log.info(LogManager.getHeader(context, "move_item", "Updating item with inherited policies"));
                inheritCollectionDefaultPolicies(context, item, to);
            }
            context.turnOffAuthorisationSystem();
            update(context, item);
            context.restoreAuthSystemState();
        } else {
            context.addEvent(new Event(Event.MODIFY, Constants.ITEM, item.getID(), null, getIdentifiers(context, item)));
        }
    }

    @Override
    public boolean hasUploadedFiles(Item item) throws SQLException {
        List<Bundle> bundles = getBundles(item, "ORIGINAL");
        for (Bundle bundle : bundles) {
            if (CollectionUtils.isNotEmpty(bundle.getBitstreams())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public List<Collection> getCollectionsNotLinked(Context context, Item item) throws SQLException {
        List<Collection> allCollections = collectionService.findAll(context);
        List<Collection> linkedCollections = item.getCollections();
        List<Collection> notLinkedCollections = new ArrayList<>(allCollections.size() - linkedCollections.size());
        if ((allCollections.size() - linkedCollections.size()) == 0) {
            return notLinkedCollections;
        }
        for (Collection collection : allCollections) {
            boolean alreadyLinked = false;
            for (Collection linkedCommunity : linkedCollections) {
                if (collection.getID().equals(linkedCommunity.getID())) {
                    alreadyLinked = true;
                    break;
                }
            }
            if (!alreadyLinked) {
                notLinkedCollections.add(collection);
            }
        }
        return notLinkedCollections;
    }

    @Override
    public boolean canEdit(Context context, Item item) throws SQLException {
        if (authorizeService.authorizeActionBoolean(context, item, Constants.WRITE)) {
            return true;
        }
        if (item.getOwningCollection() == null) {
            return true;
        }
        return collectionService.canEditBoolean(context, item.getOwningCollection(), false);
    }

    protected List<ResourcePolicy> filterPoliciesToAdd(Context context, List<ResourcePolicy> defaultCollectionPolicies, DSpaceObject dso) throws SQLException, AuthorizeException {
        List<ResourcePolicy> policiesToAdd = new ArrayList<>();
        for (ResourcePolicy defaultCollectionPolicy : defaultCollectionPolicies) {
            ResourcePolicy rp = (ResourcePolicy) resourcePolicyService.clone(context, defaultCollectionPolicy);
            rp.setAction(Constants.READ);
            if (!authorizeService.isAnIdenticalPolicyAlreadyInPlace(context, dso, rp)) {
                rp.setRpType(ResourcePolicy.TYPE_INHERITED);
                policiesToAdd.add(rp);
            }
        }
        return policiesToAdd;
    }

    /**
     * Returns an iterator of Items possessing the passed metadata field, or only
     * those matching the passed value, if value is not Item.ANY
     *
     * @param context DSpace context object
     * @param schema metadata field schema
     * @param element metadata field element
     * @param qualifier metadata field qualifier
     * @param value field value or Item.ANY to match any value
     * @return an iterator over the items matching that authority value
     * @throws SQLException, AuthorizeException, IOException
     *
     */
    @Override
    public Iterator<Item> findByMetadataField(Context context, String schema, String element, String qualifier, String value) throws SQLException, AuthorizeException, IOException {
        MetadataSchema mds = metadataSchemaService.find(context, schema);
        if (mds == null) {
            throw new IllegalArgumentException("No such metadata schema: " + schema);
        }
        MetadataField mdf = metadataFieldService.findByElement(context, mds, element, qualifier);
        if (mdf == null) {
            throw new IllegalArgumentException("No such metadata field: schema=" + schema + ", element=" + element + ", qualifier=" + qualifier);
        }
        if (Item.ANY.equals(value)) {
            return itemDAO.findByMetadataField(context, mdf, null, true);
        } else {
            return itemDAO.findByMetadataField(context, mdf, value, true);
        }
    }

    @Override
    public Iterator<Item> findByMetadataQuery(Context context, List<List<MetadataField>> listFieldList, List<String> query_op, List<String> query_val, List<UUID> collectionUuids, String regexClause, int offset, int limit) throws SQLException, AuthorizeException, IOException {
        return itemDAO.findByMetadataQuery(context, listFieldList, query_op, query_val, collectionUuids, regexClause, offset, limit);
    }

    @Override
    public DSpaceObject getAdminObject(Context context, Item item, int action) throws SQLException {
        DSpaceObject adminObject = null;
        Collection collection = (Collection) getParentObject(context, item);
        Community community = null;
        if (collection != null) {
            if (CollectionUtils.isNotEmpty(collection.getCommunities())) {
                community = collection.getCommunities().get(0);
            }
        }
        switch(action) {
            case Constants.ADD:
                if (AuthorizeConfiguration.canItemAdminPerformBitstreamCreation()) {
                    adminObject = item;
                } else {
                    if (AuthorizeConfiguration.canCollectionAdminPerformBitstreamCreation()) {
                        adminObject = collection;
                    } else {
                        if (AuthorizeConfiguration.canCommunityAdminPerformBitstreamCreation()) {
                            adminObject = community;
                        }
                    }
                }
                break;
            case Constants.REMOVE:
                if (AuthorizeConfiguration.canItemAdminPerformBitstreamDeletion()) {
                    adminObject = item;
                } else {
                    if (AuthorizeConfiguration.canCollectionAdminPerformBitstreamDeletion()) {
                        adminObject = collection;
                    } else {
                        if (AuthorizeConfiguration.canCommunityAdminPerformBitstreamDeletion()) {
                            adminObject = community;
                        }
                    }
                }
                break;
            case Constants.DELETE:
                if (item.getOwningCollection() != null) {
                    if (AuthorizeConfiguration.canCollectionAdminPerformItemDeletion()) {
                        adminObject = collection;
                    } else {
                        if (AuthorizeConfiguration.canCommunityAdminPerformItemDeletion()) {
                            adminObject = community;
                        }
                    }
                } else {
                    if (AuthorizeConfiguration.canCollectionAdminManageTemplateItem()) {
                        adminObject = collection;
                    } else {
                        if (AuthorizeConfiguration.canCommunityAdminManageCollectionTemplateItem()) {
                            adminObject = community;
                        }
                    }
                }
                break;
            case Constants.WRITE:
                if (item.getOwningCollection() == null) {
                    if (AuthorizeConfiguration.canCollectionAdminManageTemplateItem()) {
                        adminObject = collection;
                    } else {
                        if (AuthorizeConfiguration.canCommunityAdminManageCollectionTemplateItem()) {
                            adminObject = community;
                        }
                    }
                } else {
                    adminObject = item;
                }
                break;
            default:
                adminObject = item;
                break;
        }
        return adminObject;
    }

    @Override
    public DSpaceObject getParentObject(Context context, Item item) throws SQLException {
        Collection ownCollection = item.getOwningCollection();
        if (ownCollection != null) {
            return ownCollection;
        } else {
            return item.getTemplateItemOf();
        }
    }

    @Override
    public Iterator<Item> findByAuthorityValue(Context context, String schema, String element, String qualifier, String value) throws SQLException, AuthorizeException {
        MetadataSchema mds = metadataSchemaService.find(context, schema);
        if (mds == null) {
            throw new IllegalArgumentException("No such metadata schema: " + schema);
        }
        MetadataField mdf = metadataFieldService.findByElement(context, mds, element, qualifier);
        if (mdf == null) {
            throw new IllegalArgumentException("No such metadata field: schema=" + schema + ", element=" + element + ", qualifier=" + qualifier);
        }
        return itemDAO.findByAuthorityValue(context, mdf, value, true);
    }

    @Override
    public Iterator<Item> findByMetadataFieldAuthority(Context context, String mdString, String authority) throws SQLException, AuthorizeException {
        String[] elements = getElementsFilled(mdString);
        String schema = elements[0], element = elements[1], qualifier = elements[2];
        MetadataSchema mds = metadataSchemaService.find(context, schema);
        if (mds == null) {
            throw new IllegalArgumentException("No such metadata schema: " + schema);
        }
        MetadataField mdf = metadataFieldService.findByElement(context, mds, element, qualifier);
        if (mdf == null) {
            throw new IllegalArgumentException("No such metadata field: schema=" + schema + ", element=" + element + ", qualifier=" + qualifier);
        }
        return findByAuthorityValue(context, mds.getName(), mdf.getElement(), mdf.getQualifier(), authority);
    }

    @Override
    public boolean isItemListedForUser(Context context, Item item) {
        try {
            if (authorizeService.isAdmin(context)) {
                return true;
            }
            if (authorizeService.authorizeActionBoolean(context, item, org.dspace.core.Constants.READ)) {
                if (item.isDiscoverable()) {
                    return true;
                }
            }
            log.debug("item(" + item.getID() + ") " + item.getName() + " is unlisted.");
            return false;
        } catch (SQLException e) {
            log.error(e.getMessage());
            return false;
        }
    }

    @Override
    public int countItems(Context context, Collection collection) throws SQLException {
        return itemDAO.countItems(context, collection, true, false);
    }

    @Override
    public int countItems(Context context, Community community) throws SQLException {
        List<Collection> collections = communityService.getAllCollections(context, community);
        return itemDAO.countItems(context, collections, true, false);
    }

    @Override
    protected void getAuthoritiesAndConfidences(String fieldKey, Collection collection, List<String> values, List<String> authorities, List<Integer> confidences, int i) {
        Choices c = choiceAuthorityService.getBestMatch(fieldKey, values.get(i), collection, null);
        authorities.add(c.values.length > 0 ? c.values[0].authority : null);
        confidences.add(c.confidence);
    }

    @Override
    public Item findByIdOrLegacyId(Context context, String id) throws SQLException {
        if (StringUtils.isNumeric(id)) {
            return findByLegacyId(context, Integer.parseInt(id));
        } else {
            return find(context, UUID.fromString(id));
        }
    }

    @Override
    public Item findByLegacyId(Context context, int id) throws SQLException {
        return itemDAO.findByLegacyId(context, id, Item.class);
    }

    @Override
    public Iterator<Item> findByLastModifiedSince(Context context, Date last) throws SQLException {
        return itemDAO.findByLastModifiedSince(context, last);
    }

    @Override
    public int countTotal(Context context) throws SQLException {
        return itemDAO.countRows(context);
    }

    @Override
    public int countNotArchivedItems(Context context) throws SQLException {
        return itemDAO.countItems(context, false, false);
    }

    @Override
    public int countWithdrawnItems(Context context) throws SQLException {
        return itemDAO.countItems(context, false, true);
    }
}
