CREATE FUNCTION Security.fn_SecurityPredicateByDivision(@Division AS VARCHAR(50))
    RETURNS TABLE
    WITH SCHEMABINDING
AS
    RETURN 
    SELECT 1 AS result
    WHERE 
        -- User's division matches row's division
        @Division IN (
            SELECT Division 
            FROM Security.UserDivisionMapping 
            WHERE UserEmail = USER_NAME() 
              AND IsActive = 1
        )
        -- OR user has admin access (Division = 'ALL')
        OR EXISTS (
            SELECT 1 
            FROM Security.UserDivisionMapping 
            WHERE UserEmail = USER_NAME() 
              AND Division = 'ALL' 
              AND IsActive = 1
        );