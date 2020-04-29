#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'Stephen Stokes: sstokes[at]relationshipvelocity.com'

import json
import logging

import tools.helpers as h

# Logging statements for each module:
log = logging.getLogger(__name__)
log.debug('Logging is configured.')


# Primary function
@h.timer(log)
def do_query():
    sf_rest = h.get_sf_rest_connection('./config/prs.prd.json')

    # query = "select Id, Name, RecordType.Name, ParentId, Parent.Legacy_ID__c, EIN__c from Account"
    # query = "select Id, Name, SobjectType from RecordType where SobjectType = 'Account'"
    # query = "select count(Id) from Account where Legacy_ID__c in ('0013900001PP4E7AAL', '00170000010p2tdAAA')"
    # query = "select Legacy_ID__c, Parent.Legacy_ID__c from Account where Legacy_ID__c in ('0013900001PP4E7AAL', '00170000010p2tdAAA')"
    # query = "select Legacy_ID__c, Parent.Legacy_ID__c from Account where Legacy_ID__c != null"
    # query = "select Name, Type, RecordTypeId, BillingStreet, BillingCity, BillingState, BillingPostalCode, BillingCountry, BillingLatitude, BillingLongitude, BillingGeocodeAccuracy, ShippingStreet, ShippingCity, ShippingState, ShippingPostalCode, ShippingCountry, ShippingLatitude, ShippingLongitude, ShippingGeocodeAccuracy, Phone, Fax, AccountNumber, Website, Sic, Industry, AnnualRevenue, NumberOfEmployees, Ownership, TickerSymbol, Description, Rating, Site, AccountSource, SicDesc, BIS_External_ID__c, Controlled_Grp__c, EIN__c, Fiscal_Year_End__c, Last_Update_User__c, NAICS_Code__c, Client_External_ID__c, DTP_Data_Quality__c, Direct_Contact__c, Acts_as_an_Advisor_Firm__c, Partner_External_ID__c, Custodian_External_ID__c, Trading_Partner_External_ID__c, Advisor_External_ID__c, Last_Update_Date__c, Employer_Division_Restriction__c, Holding_Company__c, Legacy_ID__c, Market_Segment__c, Membership__c, Notes__c, Number_of_Locations__c, Party_Brand__c, Selling_Agreement_Status__c, Vanity_URL__c, Web_Address__c, LID__LinkedIn_Company_Id__c from Account where Legacy_ID__c != null"
    # query = "select Id from Account where Id in (select Id from Account where Legacy_ID__c != null) and ParentId != null"
    # obj = 'WS_Feature_Flag__c'
    # query = f'select count(Id) from {obj}'
    # query = 'select Username, LastName, FirstName, MiddleName, Suffix, CompanyName, Division, Department, Title, Street, City, State, PostalCode, Country, Latitude, Longitude, GeocodeAccuracy, Email, EmailPreferencesAutoBcc, EmailPreferencesAutoBccStayInTouch, EmailPreferencesStayInTouchReminder, SenderEmail, SenderName, Signature, StayInTouchSubject, StayInTouchSignature, StayInTouchNote, Phone, Fax, MobilePhone, Alias, CommunityNickname, IsActive, TimeZoneSidKey, LocaleSidKey, ReceivesInfoEmails, ReceivesAdminInfoEmails, EmailEncodingKey, LanguageLocaleKey, EmployeeNumber, UserPermissionsMarketingUser, UserPermissionsOfflineUser, UserPermissionsAvantgoUser, UserPermissionsCallCenterAutoLogin, UserPermissionsSFContentUser, UserPermissionsKnowledgeUser, UserPermissionsInteractionUser, UserPermissionsSupportUser, ForecastEnabled, UserPreferencesActivityRemindersPopup, UserPreferencesEventRemindersCheckboxDefault, UserPreferencesTaskRemindersCheckboxDefault, UserPreferencesReminderSoundOff, UserPreferencesDisableAllFeedsEmail, UserPreferencesDisableFollowersEmail, UserPreferencesDisableProfilePostEmail, UserPreferencesDisableChangeCommentEmail, UserPreferencesDisableLaterCommentEmail, UserPreferencesDisProfPostCommentEmail, UserPreferencesApexPagesDeveloperMode, UserPreferencesReceiveNoNotificationsAsApprover, UserPreferencesReceiveNotificationsAsDelegatedApprover, UserPreferencesHideCSNGetChatterMobileTask, UserPreferencesDisableMentionsPostEmail, UserPreferencesDisMentionsCommentEmail, UserPreferencesHideCSNDesktopTask, UserPreferencesHideChatterOnboardingSplash, UserPreferencesHideSecondChatterOnboardingSplash, UserPreferencesDisCommentAfterLikeEmail, UserPreferencesDisableLikeEmail, UserPreferencesSortFeedByComment, UserPreferencesDisableMessageEmail, UserPreferencesDisableBookmarkEmail, UserPreferencesDisableSharePostEmail, UserPreferencesEnableAutoSubForFeeds, UserPreferencesDisableFileShareNotificationsForApi, UserPreferencesShowTitleToExternalUsers, UserPreferencesShowManagerToExternalUsers, UserPreferencesShowEmailToExternalUsers, UserPreferencesShowWorkPhoneToExternalUsers, UserPreferencesShowMobilePhoneToExternalUsers, UserPreferencesShowFaxToExternalUsers, UserPreferencesShowStreetAddressToExternalUsers, UserPreferencesShowCityToExternalUsers, UserPreferencesShowStateToExternalUsers, UserPreferencesShowPostalCodeToExternalUsers, UserPreferencesShowCountryToExternalUsers, UserPreferencesShowProfilePicToGuestUsers, UserPreferencesShowTitleToGuestUsers, UserPreferencesShowCityToGuestUsers, UserPreferencesShowStateToGuestUsers, UserPreferencesShowPostalCodeToGuestUsers, UserPreferencesShowCountryToGuestUsers, UserPreferencesPipelineViewHideHelpPopover, UserPreferencesHideS1BrowserUI, UserPreferencesDisableEndorsementEmail, UserPreferencesPathAssistantCollapsed, UserPreferencesCacheDiagnostics, UserPreferencesShowEmailToGuestUsers, UserPreferencesShowManagerToGuestUsers, UserPreferencesShowWorkPhoneToGuestUsers, UserPreferencesShowMobilePhoneToGuestUsers, UserPreferencesShowFaxToGuestUsers, UserPreferencesShowStreetAddressToGuestUsers, UserPreferencesLightningExperiencePreferred, UserPreferencesPreviewLightning, UserPreferencesLightningConsolePreferred, UserPreferencesHideEndUserOnboardingAssistantModal, UserPreferencesHideLightningMigrationModal, UserPreferencesHideSfxWelcomeMat, UserPreferencesHideBiggerPhotoCallout, UserPreferencesGlobalNavBarWTShown, UserPreferencesGlobalNavGridMenuWTShown, UserPreferencesCreateLEXAppsWTShown, UserPreferencesFavoritesWTShown, UserPreferencesRecordHomeSectionCollapseWTShown, UserPreferencesRecordHomeReservedWTShown, UserPreferencesFavoritesShowTopFavorites, UserPreferencesExcludeMailAppAttachments, UserPreferencesSuppressTaskSFXReminders, UserPreferencesSuppressEventSFXReminders, UserPreferencesPreviewCustomTheme, UserPreferencesHasCelebrationBadge, UserPreferencesUserDebugModePref, UserPreferencesSRHOverrideActivities, UserPreferencesNewLightningReportRunPageEnabled, UserPreferencesNativeEmailClient, Extension, FederationIdentifier, AboutMe, DigestFrequency, DefaultGroupNotificationFrequency, FinServWaveExt__WavePermissions__c, FinServ__ReferrerScore__c, Pentegra_ID__c, AD_Username__c, Legacy_ID__c, copado__Hide_Copado_feedback__c, copado__Show_Copado_Tips__c, copado__Work_Manager_Panels__c, CTM_User__c, Current_Yr_Q1_Production__c, Current_Yr_Q2_Production__c, Current_Yr_Q3_Production__c, Current_Yr_Q4_Production__c, Goal__c, Primary_Role__c, Prior_Year_Q1__c, Prior_Year_Q2__c, Prior_Year_Q3__c, Prior_Year_Q4__c, Prior_Year_TPA_ICP__c, Quota__c, Reporting_Department__c, Target__c, Total_Booked_Business__c, Uber__c, User_ID__c, Contact.UUID__c from User'
    # query = 'select Username, LastName, FirstName, MiddleName, Suffix, CompanyName, Division, Department, Title, Street, City, State, PostalCode, Country, Latitude, Longitude, GeocodeAccuracy, Email, EmailPreferencesAutoBcc, EmailPreferencesAutoBccStayInTouch, EmailPreferencesStayInTouchReminder, SenderEmail, SenderName, Signature, StayInTouchSubject, StayInTouchSignature, StayInTouchNote, Phone, Fax, MobilePhone, Alias, CommunityNickname, IsActive, TimeZoneSidKey, LocaleSidKey, ReceivesInfoEmails, ReceivesAdminInfoEmails, EmailEncodingKey, LanguageLocaleKey, EmployeeNumber, UserPermissionsMarketingUser, UserPermissionsOfflineUser, UserPermissionsAvantgoUser, UserPermissionsCallCenterAutoLogin, UserPermissionsSFContentUser, UserPermissionsKnowledgeUser, UserPermissionsInteractionUser, UserPermissionsSupportUser, ForecastEnabled, UserPreferencesActivityRemindersPopup, UserPreferencesEventRemindersCheckboxDefault, UserPreferencesTaskRemindersCheckboxDefault, UserPreferencesReminderSoundOff, UserPreferencesDisableAllFeedsEmail, UserPreferencesDisableFollowersEmail, UserPreferencesDisableProfilePostEmail, UserPreferencesDisableChangeCommentEmail, UserPreferencesDisableLaterCommentEmail, UserPreferencesDisProfPostCommentEmail, UserPreferencesApexPagesDeveloperMode, UserPreferencesReceiveNoNotificationsAsApprover, UserPreferencesReceiveNotificationsAsDelegatedApprover, UserPreferencesHideCSNGetChatterMobileTask, UserPreferencesDisableMentionsPostEmail, UserPreferencesDisMentionsCommentEmail, UserPreferencesHideCSNDesktopTask, UserPreferencesHideChatterOnboardingSplash, UserPreferencesHideSecondChatterOnboardingSplash, UserPreferencesDisCommentAfterLikeEmail, UserPreferencesDisableLikeEmail, UserPreferencesSortFeedByComment, UserPreferencesDisableMessageEmail, UserPreferencesDisableBookmarkEmail, UserPreferencesDisableSharePostEmail, UserPreferencesEnableAutoSubForFeeds, UserPreferencesDisableFileShareNotificationsForApi, UserPreferencesShowTitleToExternalUsers, UserPreferencesShowManagerToExternalUsers, UserPreferencesShowEmailToExternalUsers, UserPreferencesShowWorkPhoneToExternalUsers, UserPreferencesShowMobilePhoneToExternalUsers, UserPreferencesShowFaxToExternalUsers, UserPreferencesShowStreetAddressToExternalUsers, UserPreferencesShowCityToExternalUsers, UserPreferencesShowStateToExternalUsers, UserPreferencesShowPostalCodeToExternalUsers, UserPreferencesShowCountryToExternalUsers, UserPreferencesShowProfilePicToGuestUsers, UserPreferencesShowTitleToGuestUsers, UserPreferencesShowCityToGuestUsers, UserPreferencesShowStateToGuestUsers, UserPreferencesShowPostalCodeToGuestUsers, UserPreferencesShowCountryToGuestUsers, UserPreferencesPipelineViewHideHelpPopover, UserPreferencesHideS1BrowserUI, UserPreferencesDisableEndorsementEmail, UserPreferencesPathAssistantCollapsed, UserPreferencesCacheDiagnostics, UserPreferencesShowEmailToGuestUsers, UserPreferencesShowManagerToGuestUsers, UserPreferencesShowWorkPhoneToGuestUsers, UserPreferencesShowMobilePhoneToGuestUsers, UserPreferencesShowFaxToGuestUsers, UserPreferencesShowStreetAddressToGuestUsers, UserPreferencesLightningExperiencePreferred, UserPreferencesPreviewLightning, UserPreferencesLightningConsolePreferred, UserPreferencesHideEndUserOnboardingAssistantModal, UserPreferencesHideLightningMigrationModal, UserPreferencesHideSfxWelcomeMat, UserPreferencesHideBiggerPhotoCallout, UserPreferencesGlobalNavBarWTShown, UserPreferencesGlobalNavGridMenuWTShown, UserPreferencesCreateLEXAppsWTShown, UserPreferencesFavoritesWTShown, UserPreferencesRecordHomeSectionCollapseWTShown, UserPreferencesRecordHomeReservedWTShown, UserPreferencesFavoritesShowTopFavorites, UserPreferencesExcludeMailAppAttachments, UserPreferencesSuppressTaskSFXReminders, UserPreferencesSuppressEventSFXReminders, UserPreferencesPreviewCustomTheme, UserPreferencesHasCelebrationBadge, UserPreferencesUserDebugModePref, UserPreferencesSRHOverrideActivities, UserPreferencesNewLightningReportRunPageEnabled, UserPreferencesNativeEmailClient, Extension, Pentegra_ID__c, AD_Username__c, Legacy_ID__c, User_ID__c, ContactId from User'
    query = "select Id, Name from Account where Name = 'Wells Fargo'"

    results = sf_rest.soql_query(query)

    # url = "/tooling/executeAnonymous/?anonymousBody=Database.executeBatch(new UuidUtils());"
    # results = sf_rest.get_response(url).json()

    with open('./output/query.json', 'w') as json_file:
        json.dump(results, json_file)
    sf_rest.close_connection()

    return len(results)


# Run main program
if __name__ == '__main__':
    h.setup_logging(level=logging.DEBUG)
    log.info(do_query())
