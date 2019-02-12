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
import org.dspace.content.dao.CollectionDAO;
import org.dspace.content.service.*;
import org.dspace.core.*;
import org.dspace.core.service.LicenseService;
import org.dspace.eperson.Group;
import org.dspace.eperson.service.GroupService;
import org.dspace.eperson.service.SubscribeService;
import org.dspace.event.Event;
import org.dspace.harvest.HarvestedCollection;
import org.dspace.harvest.service.HarvestedCollectionService;
import org.dspace.workflow.factory.WorkflowServiceFactory;
import org.springframework.beans.factory.annotation.Autowired;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.*;

/**
 * Service implementation for the Collection object.
 * This class is responsible for all business logic calls for the Collection object and is autowired by spring.
 * This class should never be accessed directly.
 *
 * @author kevinvandevelde at atmire.com
 */
public class CollectionServiceImpl extends DSpaceObjectServiceImpl<Collection> implements CollectionService {

    /** log4j category */
    private static final Logger log = Logger.getLogger(CollectionServiceImpl.class);

    @Autowired(required = true)
    protected CollectionDAO collectionDAO;

    @Autowired(required = true)
    protected AuthorizeService authorizeService;

    @Autowired(required = true)
    protected BitstreamService bitstreamService;

    @Autowired(required = true)
    protected ItemService itemService;

    @Autowired(required = true)
    protected CommunityService communityService;

    @Autowired(required = true)
    protected GroupService groupService;

    @Autowired(required = true)
    protected LicenseService licenseService;

    @Autowired(required = true)
    protected SubscribeService subscribeService;

    @Autowired(required = true)
    protected WorkspaceItemService workspaceItemService;

    @Autowired(required = true)
    protected HarvestedCollectionService harvestedCollectionService;

    protected CollectionServiceImpl() {
        super();
    }

    @Override
    public Collection create(Context context, Community community) throws SQLException, AuthorizeException {
        return create(context, community, null);
    }

    @Override
    public Collection create(Context context, Community community, String handle) throws SQLException, AuthorizeException {
        if (community == null) {
            throw new IllegalArgumentException("Community cannot be null when creating a new collection.");
        }
        Collection newCollection = collectionDAO.create(context, new Collection());
        communityService.addCollection(context, community, newCollection);
        if (handle == null) {
            handleService.createHandle(context, newCollection);
        } else {
            handleService.createHandle(context, newCollection, handle);
        }
        Group anonymousGroup = groupService.findByName(context, Group.ANONYMOUS);
        authorizeService.createResourcePolicy(context, newCollection, anonymousGroup, null, Constants.READ, null);
        authorizeService.createResourcePolicy(context, newCollection, anonymousGroup, null, Constants.DEFAULT_ITEM_READ, null);
        authorizeService.createResourcePolicy(context, newCollection, anonymousGroup, null, Constants.DEFAULT_BITSTREAM_READ, null);
        context.addEvent(new Event(Event.CREATE, Constants.COLLECTION, newCollection.getID(), newCollection.getHandle(), getIdentifiers(context, newCollection)));
        log.info(LogManager.getHeader(context, "create_collection", "collection_id=" + newCollection.getID()) + ",handle=" + newCollection.getHandle());
        collectionDAO.save(context, newCollection);
        return newCollection;
    }

    @Override
    public List<Collection> findAll(Context context) throws SQLException {
        MetadataField nameField = metadataFieldService.findByElement(context, "dc", "title", null);
        return collectionDAO.findAll(context, nameField);
    }

    @Override
    public List<Collection> findAll(Context context, Integer limit, Integer offset) throws SQLException {
        MetadataField nameField = metadataFieldService.findByElement(context, "dc", "title", null);
        return collectionDAO.findAll(context, nameField, limit, offset);
    }

    @Override
    public List<Collection> findAuthorizedOptimized(Context context, int actionID) throws SQLException {
        if (!ConfigurationManager.getBooleanProperty("org.dspace.content.Collection.findAuthorizedPerformanceOptimize", false)) {
            return findAuthorized(context, null, actionID);
        }
        List<Collection> myResults = new ArrayList<>();
        if (authorizeService.isAdmin(context)) {
            return findAll(context);
        }
        List<Collection> directToCollection = findDirectMapped(context, actionID);
        for (int i = 0; i < directToCollection.size(); i++) {
            if (!myResults.contains(directToCollection.get(i))) {
                myResults.add(directToCollection.get(i));
            }
        }
        List<Collection> groupToCollection = findGroupMapped(context, actionID);
        for (Collection aGroupToCollection : groupToCollection) {
            if (!myResults.contains(aGroupToCollection)) {
                myResults.add(aGroupToCollection);
            }
        }
        List<Collection> group2GroupToCollection = findGroup2GroupMapped(context, actionID);
        for (Collection aGroup2GroupToCollection : group2GroupToCollection) {
            if (!myResults.contains(aGroup2GroupToCollection)) {
                myResults.add(aGroup2GroupToCollection);
            }
        }
        List<Collection> group2commCollections = findGroup2CommunityMapped(context);
        for (Collection group2commCollection : group2commCollections) {
            if (!myResults.contains(group2commCollection)) {
                myResults.add(group2commCollection);
            }
        }
        Collections.sort(myResults, new CollectionNameComparator());
        return myResults;
    }

    @Override
    public List<Collection> findDirectMapped(Context context, int actionID) throws SQLException {
        return collectionDAO.findAuthorized(context, context.getCurrentUser(), Arrays.asList(Constants.ADD, Constants.ADMIN));
    }

    @Override
    public List<Collection> findGroup2CommunityMapped(Context context) throws SQLException {
        List<Community> communities = communityService.findAuthorizedGroupMapped(context, Arrays.asList(Constants.ADD, Constants.ADMIN));
        List<Collection> collections = new ArrayList<>();
        for (Community community : communities) {
            collections.addAll(community.getCollections());
        }
        return collections;
    }

    @Override
    public List<Collection> findGroup2GroupMapped(Context context, int actionID) throws SQLException {
        return collectionDAO.findAuthorizedByGroup(context, context.getCurrentUser(), Collections.singletonList(actionID));
    }

    @Override
    public List<Collection> findGroupMapped(Context context, int actionID) throws SQLException {
        List<Community> communities = communityService.findAuthorized(context, Arrays.asList(Constants.ADD, Constants.ADMIN));
        List<Collection> collections = new ArrayList<>();
        for (Community community : communities) {
            collections.addAll(community.getCollections());
        }
        return collections;
    }

    @Override
    public Collection find(Context context, UUID id) throws SQLException {
        return collectionDAO.findByID(context, Collection.class, id);
    }

    @Override
    public void setMetadata(Context context, Collection collection, String field, String value) throws MissingResourceException, SQLException {
        if ((field.trim()).equals("name") && (value == null || value.trim().equals(""))) {
            try {
                value = I18nUtil.getMessage("org.dspace.workflow.WorkflowManager.untitled");
            } catch (MissingResourceException e) {
                value = "Untitled";
            }
        }
        String[] MDValue = getMDValueByLegacyField(field);
        if (value == null) {
            clearMetadata(context, collection, MDValue[0], MDValue[1], MDValue[2], Item.ANY);
            collection.setMetadataModified();
        } else {
            setMetadataSingleValue(context, collection, MDValue[0], MDValue[1], MDValue[2], null, value);
        }
        collection.addDetails(field);
    }

    @Override
    public Bitstream setLogo(Context context, Collection collection, InputStream is) throws AuthorizeException, IOException, SQLException {
        if (!((is == null) && authorizeService.authorizeActionBoolean(context, collection, Constants.DELETE))) {
            canEdit(context, collection, true);
        }
        if (collection.getLogo() != null) {
            bitstreamService.delete(context, collection.getLogo());
        }
        if (is == null) {
            collection.setLogo(null);
            log.info(LogManager.getHeader(context, "remove_logo", "collection_id=" + collection.getID()));
        } else {
            Bitstream newLogo = bitstreamService.create(context, is);
            collection.setLogo(newLogo);
            List<ResourcePolicy> policies = authorizeService.getPoliciesActionFilter(context, collection, Constants.READ);
            authorizeService.addPolicies(context, policies, newLogo);
            log.info(LogManager.getHeader(context, "set_logo", "collection_id=" + collection.getID() + "logo_bitstream_id=" + newLogo.getID()));
        }
        collection.setModified();
        return collection.getLogo();
    }

    @Override
    public Group createWorkflowGroup(Context context, Collection collection, int step) throws SQLException, AuthorizeException {
        AuthorizeUtil.authorizeManageWorkflowsGroup(context, collection);
        if (getWorkflowGroup(collection, step) == null) {
            context.turnOffAuthorisationSystem();
            Group g = groupService.create(context);
            context.restoreAuthSystemState();
            groupService.setName(g, "COLLECTION_" + collection.getID() + "_WORKFLOW_STEP_" + step);
            groupService.update(context, g);
            setWorkflowGroup(collection, step, g);
            authorizeService.addPolicy(context, collection, Constants.ADD, g);
        }
        return getWorkflowGroup(collection, step);
    }

    @Override
    public void setWorkflowGroup(Collection collection, int step, Group group) {
        switch(step) {
            case 1:
                collection.setWorkflowStep1(group);
                break;
            case 2:
                collection.setWorkflowStep2(group);
                break;
            case 3:
                collection.setWorkflowStep3(group);
                break;
            default:
                throw new IllegalArgumentException("Illegal step count: " + step);
        }
    }

    @Override
    public Group getWorkflowGroup(Collection collection, int step) {
        switch(step) {
            case 1:
                return collection.getWorkflowStep1();
            case 2:
                return collection.getWorkflowStep2();
            case 3:
                return collection.getWorkflowStep3();
            default:
                throw new IllegalStateException("Illegal step count: " + step);
        }
    }

    /**
     * Get the value of a metadata field
     *
     * @param collection
     * @param field
     *            the name of the metadata field to get
     *
     * @return the value of the metadata field
     *
     * @exception IllegalArgumentException
     *                if the requested metadata field doesn't exist
     */
    @Override
    @Deprecated
    public String getMetadata(Collection collection, String field) {
        String[] MDValue = getMDValueByLegacyField(field);
        String value = getMetadataFirstValue(collection, MDValue[0], MDValue[1], MDValue[2], Item.ANY);
        return value == null ? "" : value;
    }

    @Override
    public Group createSubmitters(Context context, Collection collection) throws SQLException, AuthorizeException {
        AuthorizeUtil.authorizeManageSubmittersGroup(context, collection);
        Group submitters = collection.getSubmitters();
        if (submitters == null) {
            context.turnOffAuthorisationSystem();
            submitters = groupService.create(context);
            context.restoreAuthSystemState();
            groupService.setName(submitters, "COLLECTION_" + collection.getID() + "_SUBMIT");
            groupService.update(context, submitters);
        }
        collection.setSubmitters(submitters);
        authorizeService.addPolicy(context, collection, Constants.ADD, submitters);
        return submitters;
    }

    @Override
    public void removeSubmitters(Context context, Collection collection) throws SQLException, AuthorizeException {
        AuthorizeUtil.authorizeManageSubmittersGroup(context, collection);
        if (collection.getSubmitters() == null) {
            return;
        }
        collection.setSubmitters(null);
    }

    @Override
    public Group createAdministrators(Context context, Collection collection) throws SQLException, AuthorizeException {
        AuthorizeUtil.authorizeManageAdminGroup(context, collection);
        Group admins = collection.getAdministrators();
        if (admins == null) {
            context.turnOffAuthorisationSystem();
            admins = groupService.create(context);
            context.restoreAuthSystemState();
            groupService.setName(admins, "COLLECTION_" + collection.getID() + "_ADMIN");
            groupService.update(context, admins);
        }
        authorizeService.addPolicy(context, collection, Constants.ADMIN, admins);
        collection.setAdmins(admins);
        return admins;
    }

    @Override
    public void removeAdministrators(Context context, Collection collection) throws SQLException, AuthorizeException {
        AuthorizeUtil.authorizeRemoveAdminGroup(context, collection);
        Group admins = collection.getAdministrators();
        if (admins == null) {
            return;
        }
        collection.setAdmins(null);
    }

    @Override
    public String getLicense(Collection collection) {
        String license = getMetadata(collection, "license");
        if (license == null || license.trim().equals("")) {
            license = licenseService.getDefaultSubmissionLicense();
        }
        return license;
    }

    @Override
    public boolean hasCustomLicense(Collection collection) {
        String license = collection.getLicenseCollection();
        return StringUtils.isNotBlank(license);
    }

    @Override
    public void createTemplateItem(Context context, Collection collection) throws SQLException, AuthorizeException {
        AuthorizeUtil.authorizeManageTemplateItem(context, collection);
        if (collection.getTemplateItem() == null) {
            Item template = itemService.createTemplateItem(context, collection);
            collection.setTemplateItem(template);
            log.info(LogManager.getHeader(context, "create_template_item", "collection_id=" + collection.getID() + ",template_item_id=" + template.getID()));
        }
    }

    @Override
    public void removeTemplateItem(Context context, Collection collection) throws SQLException, AuthorizeException, IOException {
        AuthorizeUtil.authorizeManageTemplateItem(context, collection);
        Item template = collection.getTemplateItem();
        if (template != null) {
            log.info(LogManager.getHeader(context, "remove_template_item", "collection_id=" + collection.getID() + ",template_item_id=" + template.getID()));
            context.turnOffAuthorisationSystem();
            collection.setTemplateItem(null);
            itemService.delete(context, template);
            context.restoreAuthSystemState();
        }
        context.addEvent(new Event(Event.MODIFY, Constants.COLLECTION, collection.getID(), "remove_template_item", getIdentifiers(context, collection)));
    }

    @Override
    public void addItem(Context context, Collection collection, Item item) throws SQLException, AuthorizeException {
        authorizeService.authorizeAction(context, collection, Constants.ADD);
        log.info(LogManager.getHeader(context, "add_item", "collection_id=" + collection.getID() + ",item_id=" + item.getID()));
        if (!item.getCollections().contains(collection)) {
            item.addCollection(collection);
        }
        context.addEvent(new Event(Event.ADD, Constants.COLLECTION, collection.getID(), Constants.ITEM, item.getID(), item.getHandle(), getIdentifiers(context, collection)));
    }

    @Override
    public void removeItem(Context context, Collection collection, Item item) throws SQLException, AuthorizeException, IOException {
        authorizeService.authorizeAction(context, collection, Constants.REMOVE);
        if (item.getCollections().size() == 1) {
            itemService.delete(context, item);
        } else {
            item.removeCollection(collection);
        }
        context.addEvent(new Event(Event.REMOVE, Constants.COLLECTION, collection.getID(), Constants.ITEM, item.getID(), item.getHandle(), getIdentifiers(context, collection)));
    }

    @Override
    public void update(Context context, Collection collection) throws SQLException, AuthorizeException {
        canEdit(context, collection, true);
        log.info(LogManager.getHeader(context, "update_collection", "collection_id=" + collection.getID()));
        super.update(context, collection);
        collectionDAO.save(context, collection);
        if (collection.isModified()) {
            context.addEvent(new Event(Event.MODIFY, Constants.COLLECTION, collection.getID(), null, getIdentifiers(context, collection)));
            collection.clearModified();
        }
        if (collection.isMetadataModified()) {
            collection.clearDetails();
        }
    }

    @Override
    public boolean canEditBoolean(Context context, Collection collection) throws SQLException {
        return canEditBoolean(context, collection, true);
    }

    @Override
    public boolean canEditBoolean(Context context, Collection collection, boolean useInheritance) throws SQLException {
        try {
            canEdit(context, collection, useInheritance);
            return true;
        } catch (AuthorizeException e) {
            return false;
        }
    }

    @Override
    public void canEdit(Context context, Collection collection) throws SQLException, AuthorizeException {
        canEdit(context, collection, true);
    }

    @Override
    public void canEdit(Context context, Collection collection, boolean useInheritance) throws SQLException, AuthorizeException {
        List<Community> parents = collection.getCommunities();
        for (Community parent : parents) {
            if (authorizeService.authorizeActionBoolean(context, parent, Constants.WRITE, useInheritance)) {
                return;
            }
            if (authorizeService.authorizeActionBoolean(context, parent, Constants.ADD, useInheritance)) {
                return;
            }
        }
        authorizeService.authorizeAction(context, collection, Constants.WRITE, useInheritance);
    }

    @Override
    public void delete(Context context, Collection collection) throws SQLException, AuthorizeException, IOException {
        log.info(LogManager.getHeader(context, "delete_collection", "collection_id=" + collection.getID()));
        HarvestedCollection hc = harvestedCollectionService.find(context, collection);
        if (hc != null) {
            harvestedCollectionService.delete(context, hc);
        }
        context.addEvent(new Event(Event.DELETE, Constants.COLLECTION, collection.getID(), collection.getHandle(), getIdentifiers(context, collection)));
        subscribeService.deleteByCollection(context, collection);
        removeTemplateItem(context, collection);
        Iterator<Item> items = itemService.findAllByCollection(context, collection);
        while (items.hasNext()) {
            Item item = items.next();
            if (itemService.isOwningCollection(item, collection)) {
                itemService.delete(context, item);
            } else {
                removeItem(context, collection, item);
            }
        }
        setLogo(context, collection, null);
        Iterator<WorkspaceItem> workspaceItems = workspaceItemService.findByCollection(context, collection).iterator();
        while (workspaceItems.hasNext()) {
            WorkspaceItem workspaceItem = workspaceItems.next();
            workspaceItems.remove();
            workspaceItemService.deleteAll(context, workspaceItem);
        }
        WorkflowServiceFactory.getInstance().getWorkflowService().deleteCollection(context, collection);
        WorkflowServiceFactory.getInstance().getWorkflowItemService().deleteByCollection(context, collection);
        handleService.unbindHandle(context, collection);
        Group g = collection.getWorkflowStep1();
        if (g != null) {
            collection.setWorkflowStep1(null);
            groupService.delete(context, g);
        }
        g = collection.getWorkflowStep2();
        if (g != null) {
            collection.setWorkflowStep2(null);
            groupService.delete(context, g);
        }
        g = collection.getWorkflowStep3();
        if (g != null) {
            collection.setWorkflowStep3(null);
            groupService.delete(context, g);
        }
        g = collection.getAdministrators();
        if (g != null) {
            collection.setAdmins(null);
            groupService.delete(context, g);
        }
        g = collection.getSubmitters();
        if (g != null) {
            collection.setSubmitters(null);
            groupService.delete(context, g);
        }
        Iterator<Community> owningCommunities = collection.getCommunities().iterator();
        while (owningCommunities.hasNext()) {
            Community owningCommunity = owningCommunities.next();
            owningCommunities.remove();
            owningCommunity.getCollections().remove(collection);
        }
        authorizeService.removeAllPolicies(context, collection);
        collectionDAO.delete(context, collection);
    }

    @Override
    public int getSupportsTypeConstant() {
        return Constants.COLLECTION;
    }

    @Override
    public List<Collection> findAuthorized(Context context, Community community, int actionID) throws SQLException {
        List<Collection> myResults = new ArrayList<>();
        List<Collection> myCollections;
        if (community != null) {
            myCollections = community.getCollections();
        } else {
            myCollections = findAll(context);
        }
        for (Collection myCollection : myCollections) {
            if (authorizeService.authorizeActionBoolean(context, myCollection, actionID)) {
                myResults.add(myCollection);
            }
        }
        return myResults;
    }

    @Override
    public Collection findByGroup(Context context, Group group) throws SQLException {
        return collectionDAO.findByGroup(context, group);
    }

    @Override
    public List<Collection> findCollectionsWithSubscribers(Context context) throws SQLException {
        return collectionDAO.findCollectionsWithSubscribers(context);
    }

    @Override
    public DSpaceObject getAdminObject(Context context, Collection collection, int action) throws SQLException {
        DSpaceObject adminObject = null;
        Community community = null;
        List<Community> communities = collection.getCommunities();
        if (CollectionUtils.isNotEmpty(communities)) {
            community = communities.iterator().next();
        }
        switch(action) {
            case Constants.REMOVE:
                if (AuthorizeConfiguration.canCollectionAdminPerformItemDeletion()) {
                    adminObject = collection;
                } else {
                    if (AuthorizeConfiguration.canCommunityAdminPerformItemDeletion()) {
                        adminObject = community;
                    }
                }
                break;
            case Constants.DELETE:
                if (AuthorizeConfiguration.canCommunityAdminPerformSubelementDeletion()) {
                    adminObject = community;
                }
                break;
            default:
                adminObject = collection;
                break;
        }
        return adminObject;
    }

    @Override
    public DSpaceObject getParentObject(Context context, Collection collection) throws SQLException {
        List<Community> communities = collection.getCommunities();
        if (CollectionUtils.isNotEmpty(communities)) {
            return communities.iterator().next();
        } else {
            return null;
        }
    }

    @Override
    public void updateLastModified(Context context, Collection collection) throws SQLException, AuthorizeException {
        context.addEvent(new Event(Event.MODIFY, Constants.COLLECTION, collection.getID(), null, getIdentifiers(context, collection)));
    }

    @Override
    public Collection findByIdOrLegacyId(Context context, String id) throws SQLException {
        if (StringUtils.isNumeric(id)) {
            return findByLegacyId(context, Integer.parseInt(id));
        } else {
            return find(context, UUID.fromString(id));
        }
    }

    @Override
    public Collection findByLegacyId(Context context, int id) throws SQLException {
        return collectionDAO.findByLegacyId(context, id, Collection.class);
    }

    @Override
    public int countTotal(Context context) throws SQLException {
        return collectionDAO.countRows(context);
    }

    @Override
    public List<Map.Entry<Collection, Long>> getCollectionsWithBitstreamSizesTotal(Context context) throws SQLException {
        return collectionDAO.getCollectionsWithBitstreamSizesTotal(context);
    }
}
