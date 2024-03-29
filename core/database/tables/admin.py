from datetime import datetime, timedelta
from random import choices
from string import ascii_letters, digits

from jose import jwt
from passlib.context import CryptContext
from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    Sequence,
    String,
    Table,
)
from sqlalchemy.orm import relationship, sessionmaker

from core.config import api_settings_config
from core.database.engine import engine
from core.database.tables.utils.commons import Base
from core.database.tables.utils.dependencies import private_pem, public_pem

ACCESS_TOKEN_EXPIRE_MINUTES = api_settings_config.security[
    "access_token_expire_minutes"
]
REFRESH_TOKEN_EXPIRE_DAYS = api_settings_config.security["refresh_token_expire_days"]

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Association table
user_policies_association = Table(
    'user_policies', Base.metadata,
    Column('user_id', Integer, ForeignKey('users.user_id')),
    Column('policy_id', Integer, ForeignKey('policies.policy_id'))
)

class User(Base):
    __tablename__ = "users"

    user_id          = Column(Integer(), Sequence('user_id_seq'), primary_key=True, index=True)
    username         = Column(String(length=75), unique=True, index=True)
    user_full_name   = Column(String(length=75), nullable=False)
    password_hash    = Column(String(), nullable=False)
    is_admin         = Column(Boolean(), nullable=False, default=False)
    access_token     = Column(String(), default="")
    refresh_token    = Column(String(), default="")
    role_id          = Column(Integer(), nullable=False, default=1)
    email_address    = Column(String(length=128), unique=True, nullable=False, index=True)
    department_id    = Column(Integer(), nullable=False, default=1)
    phone_nmber      = Column(String(length=15))
    designation      = Column(String(length=25), nullable=False, default="no department assigned")
    division_id      = Column(Integer(), nullable=False)
    tenent_id        = Column(Integer(), nullable=False)
    erp_id           = Column(Integer(), nullable=False)
    session_id       = Column(String(length=20), nullable=False, default="")
    user_status      = Column(Boolean(), nullable=False)
    enabled_services = Column(String(length=100), default="")
    policies         = relationship(
        "Policy",
        secondary=user_policies_association,
        back_populates="users",
    ) # Establish the relationship to policies through the association table

    @classmethod
    def add_user(cls, username, user_full_name, password, is_admin, role_id, email_address, department_id, phone_nmber, designation, division_id, tenent_id, erp_id, user_status, enabled_services):
        """
        Add a new user to the database.

        Args:
            username (str): The username of the new user.
            user_full_name (str): The full name of the user.
            password (str): The password for the new user.
            is_admin (bool): A flag indicating if the user has administrative privileges.
            role_id (int): The ID of the role assigned to the user.
            email_address (str): The email address of the user.
            department_id (int): The ID of the department to which the user belongs.
            phone_nmber (str): The phone number of the user.
            designation (str): The designation of the user in the organization.
            division_id (int): The ID of the division to which the user belongs.
            tenent_id (int): The tenant ID associated with the user.
            erp_id (int): The ERP ID associated with the user.
            user_status (bool): The status of the user account (active/inactive).
            enabled_services (str): A string of enabled services for the user.

        Raises:
            ValueError: If the username, email address, or phone number already exist in the database.
        """
        Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        with Session() as session:
            existing_username = session.query(cls).filter_by(username=username).first()
            if existing_username:
                raise ValueError(f"Username {username} already exists!")
            
            existing_email = session.query(cls).filter_by(email_address=email_address).first()
            if existing_email:
                raise ValueError(f"Email {email_address} already exists!")
            
            existing_phone_no = session.query(cls).filter_by(phone_nmber=phone_nmber).first()
            if existing_phone_no:
                raise ValueError(f"Contact Number {phone_nmber} already exists!")
        
            new_user = cls(
                username        = username, 
                user_full_name  = user_full_name,
                password_hash   = pwd_context.hash(password),
                is_admin        = is_admin,
                role_id         = role_id,
                email_address   = email_address,
                department_id   = department_id,
                phone_nmber     = phone_nmber,
                designation     = designation,
                division_id     = division_id,
                tenent_id       = tenent_id,
                erp_id          = erp_id,
                user_status     = user_status,
                enabled_services = enabled_services
            )

            session.add(new_user)
            session.commit()
    
    def update_policies(self, new_policies=None):
        """
        Update policies for a user by attaching and/or detaching policies.

        Args:
            new_policies (list of Policy instances): The desired final list of policies for the user.
        """
        current_policies = set(self.policies)
        new_policies_set = set(new_policies)
        
        # Policies to attach: in new_policies but not in current_policies
        attach_policies = new_policies_set - current_policies
        
        # Policies to detach: in current_policies but not in new_policies
        detach_policies = current_policies - new_policies_set

        # Attach and Detach Policies
        for policy in attach_policies:
            self.policies.append(policy)
            
        for policy in detach_policies:
            self.policies.remove(policy)

    def verify_password(self, password: str):
        """
        Verify a password against the stored password hash.

        Args:
            password (str): The password to verify.

        Returns:
            bool: True if the password matches, False otherwise.
        """
        return pwd_context.verify(password, self.password_hash)

    # Utility function to create an access token
    def add_access_token(self, user_name):
        """
        Generate and store a new access token for the user.

        Args:
            user_name (str): The username of the user.

        Returns:
            str: The generated access token.
        """
        expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
        to_encode = {"sub": user_name, "exp": expire, "_services": self.enabled_services}
        access_token = jwt.encode(to_encode, private_pem, algorithm="RS256")
        self.access_token = access_token
        return access_token

    def renew_access_token(self, access_token: str="", refresh_token: str=None):
        """
        Generate and store new refresh token for the user.

        Args:
            old_refresh_token (str, optional): The old refresh token, if available. Defaults to None.

        Returns:
            str: The generated refresh token, or None if the provided old token doesn't match the stored one.
        """
        if self.access_token == access_token and self.refresh_token == refresh_token:
            if self.verify_access_token(access_token=refresh_token): # Same method can be used for verifying refresj token
                access_token = self.add_access_token(user_name=self.user_name)
                refresh_token = self.add_refresh_token()
                return access_token, refresh_token
        
        return None, None

    def add_refresh_token(self, old_refresh_token: str=None):
        """
        Generate and store a new refresh token for the user.

        Args:
            old_refresh_token (str, optional): The old refresh token, if available. Defaults to None.

        Returns:
            str: The generated refresh token, or None if the provided old token doesn't match the stored one.
        """
        if old_refresh_token and self.refresh_token != old_refresh_token:
            return None
        
        expire = datetime.utcnow() + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)
        to_encode = {"exp": expire}
        refresh_token = jwt.encode(to_encode, private_pem, algorithm="RS256")
    
        self.refresh_token = refresh_token
        
        return refresh_token
    
    def add_session_id(self):
        """
        Generate and store a new session ID for the user.

        Returns:
            str: The generated session ID.
        """
        session_id = ''.join(choices(ascii_letters+digits, k=20))
        self.session_id = session_id
        return session_id
    
    def verify_access_token(self, access_token):
        """
        Verify the provided access token against the stored one and check its expiration.

        Args:
            access_token (str): The access token to verify.

        Returns:
            bool: True if the token is valid, False otherwise.
        """
        if self.access_token==access_token:
            try:
                return jwt.decode(token=access_token, key=public_pem, algorithms="RS256")
            except jwt.ExpiredSignatureError:
                return False  # token has expired
            except jwt.JWTError:
                return False
            except:
                return False
        return False
    
    def get_enabled_services(self, access_token):
        """
        Retrieve enabled services for the user if the provided access token is valid.

        Args:
            access_token (str): The access token to verify.

        Returns:
            list[str]: A list of enabled services, or None if the token is invalid.
        """
        if self.verify_access_token(access_token=access_token):
            return self.enabled_services.split(",")

    def login_user(self, user_name:str):
        """
        Log in the user and generate relevant tokens and session ID.

        Args:
            user_name (str): The username of the user.

        Returns:
            tuple: A tuple containing the generated access token, refresh token, and session ID.
        """
        access_token = self.add_access_token(user_name=user_name)
        refresh_token = self.add_refresh_token()
        session_id = self.add_session_id()
        return access_token, refresh_token, session_id
    
    def terminate_session(self):
        """
        Clear the stored access token, refresh token, and session ID to log out the user.
        """
        self.access_token = ""
        self.refresh_token = ""
        self.session_id = ""

class Role(Base):
    """
    ORM class for manipulating and querying role data in the 'roles' table.

    Attributes:
        role_id (Integer): The unique identifier for a role, primary key.
        role_name (String): The name of the role, must be unique.
        hierarchy (Integer): A positive integer representing the role's position in the hierarchy.
    """
    __tablename__ = "roles"

    role_id   = Column(Integer(), Sequence('role_id_seq'), primary_key=True, index=True)
    role_name = Column(String(length=50), nullable=False, default="default_role")
    hierarchy = Column(Integer(), nullable=False, default=1)
    
    @classmethod
    def add_role(cls, role_name: str, hierarchy: int):
        """
        Add a new role to the 'roles' table.

        Args:
            role_name (str): The name of the new role to be added.
            hierarchy (int): The hierarchy level of the new role to be added.

        Raises:
            ValueError: If a role with the specified name already exists.
        """
        Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        with Session() as session:
            existing_role_name = session.query(cls).filter_by(role_name=role_name).first()
            if existing_role_name:
                raise ValueError(f"Role name {role_name} already exists!")
        
            new_role = cls(
                role_name = role_name,
                hierarchy = hierarchy
            )

            session.add(new_role)
            session.commit()
    
    @classmethod
    def update_role_hierarchy(cls, role_name: str, new_hierarchy: int):
        """
        Update the hierarchy level of the specified role.

        Args:
            role_name (str): The name of the role whose hierarchy level is to be updated.
            new_hierarchy (int): The new hierarchy level to be assigned to the role.

        Raises:
            ValueError: If a role with the specified name does not exist.
        """
        Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        with Session() as session:
            role_to_update = session.query(cls).filter_by(role_name=role_name).first()
            if not role_to_update:
                raise ValueError(f"Role name {role_name} does not exist!")

            roles_to_update = session.query(cls).filter(cls.hierarchy >= new_hierarchy).all()

            for role in roles_to_update:
                if role.role_name == role_name:
                    role.hierarchy = new_hierarchy
                else:
                    role.hierarchy += 1  # Increment the hierarchy of other roles

            session.commit()

    @classmethod
    def delete_role(cls, role_name: str):
        """
        Delete the specified role from the 'roles' table.

        Args:
            role_name (str): The name of the role to be deleted.

        Raises:
            ValueError: If a role with the specified name does not exist.
        """
        Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        with Session() as session:
            role_to_delete = session.query(cls).filter_by(role_name=role_name).first()
            if not role_to_delete:
                raise ValueError(f"Role name {role_name} does not exist!")

            hierarchy_to_fill = role_to_delete.hierarchy

            session.delete(role_to_delete)

            roles_to_update = session.query(cls).filter(cls.hierarchy > hierarchy_to_fill).all()

            for role in roles_to_update:
                role.hierarchy -= 1  # Decrement the hierarchy of other roles

            session.commit()

class Policy(Base):
    """
    ORM class for manipulating and querying policy data in the 'policies' table.

    Attributes:
        policy_id (Integer): The unique identifier for a policy, primary key.
        created_by (Integer): ID of the user who created the policy.
        created_on (Date): Date when the policy was created.
        last_updated_on (Date): Date when the policy was last updated.
        last_updated_by (Integer): ID of the user who last updated the policy.
        policy_definition (JSON): Definition of the policy in JSON format.
    """
    __tablename__ = "policies"

    policy_id          = Column(Integer(), Sequence('policy_id_seq'), primary_key=True, index=True)
    created_by         = Column(Integer(), nullable=False, default=1)
    created_on         = Column(DateTime(), nullable=False)
    last_updated_on    = Column(DateTime(), nullable=False)
    last_updated_by    = Column(Integer(), nullable=False, default=1)
    policy_definition  = Column(JSON, nullable=False, default=dict)
    users              = relationship(
        "User",
        secondary=user_policies_association,
        back_populates="policies",
    ) # Establish the relationship to users through the association table
    
    @classmethod
    def add_policy(cls, created_by: int, policy_definition: dict):
        """
        Add a new policy.

        Args:
            created_by (int): ID of the user who is creating the new policy.
            policy_definition (dict): Definition of the policy in JSON format.

        Returns:
            Policy: The created Policy instance.
        """
        Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        with Session() as session:
            new_policy = cls(
                created_by=created_by,
                created_on=datetime.now(),
                last_updated_on=datetime.now(),
                last_updated_by=created_by,
                policy_definition=policy_definition
            )

            session.add(new_policy)
            session.commit()
            return new_policy
    
    @classmethod
    def update_policy(cls, policy_id: int, updated_by: int, new_policy_definition: dict):
        """
        Update an existing policy.

        Args:
            policy_id (int): ID of the policy to be updated.
            updated_by (int): ID of the user who is updating the policy.
            new_policy_definition (dict): New definition of the policy in JSON format.

        Raises:
            ValueError: If the policy with the specified ID does not exist.
        """
        Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        with Session() as session:
            policy_to_update = session.query(cls).filter_by(policy_id=policy_id).first()

            if not policy_to_update:
                raise ValueError(f"Policy with ID {policy_id} does not exist!")
            
            policy_to_update.last_updated_by = updated_by
            policy_to_update.last_updated_on = datetime.now()
            policy_to_update.policy_definition = new_policy_definition
            
            session.commit()
    
    @classmethod
    def delete_policy(cls, policy_id: int):
        """
        Delete a policy.

        Args:
            policy_id (int): ID of the policy to be deleted.

        Raises:
            ValueError: If the policy with the specified ID does not exist.
        """
        Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        with Session() as session:
            policy_to_delete = session.query(cls).filter_by(policy_id=policy_id).first()

            if not policy_to_delete:
                raise ValueError(f"Policy with ID {policy_id} does not exist!")
            
            session.delete(policy_to_delete)
            session.commit()

class Services(Base):
    __tablename__ = "services"

    service_id    = Column(Integer(), Sequence('service_id_seq'), primary_key=True, index=True)
    service_name  = Column(String(length=25), nullable=False, unique=True, index=True)
    enabled       = Column(Boolean(), nullable=False, default=True)
    access_levels = Column(JSON(), nullable=False, default={"CREATE":True, "READ":True, "UPDATE":True, "DELETE":True, "FULL":True, "ADMIN":True})
    table_columns = Column(JSON(), nullable=False, default={"column_0": True, "column_1": True, "column_2": False})

    @classmethod
    def enable_disable_services(cls, services_status: dict):
        """
        Enable or disable multiple services based on the provided service_id: status pairs.

        Args:
            services_status (dict): Dictionary with service_id as key and status as value.

        Returns:
            None
        """
        # Extracting service_ids from the keys
        service_ids = list(services_status.keys())

        Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        with Session() as session:
            # Fetching all services that need to be updated
            services = session.query(cls).filter(cls.service_id.in_(service_ids)).all()

            # If no services are found, you might want to log this or raise an exception
            if not services:
                print("No services found for provided IDs")

            # Updating each service status
            for service in services:
                # Check if the key is present in services_status dict before updating the status
                if service.service_id in services_status.keys():
                    service.enabled = services_status[service.service_id]

            session.commit()


    def update_access_levels(cls, services_access):
        """
        Update the access levels of multiple services based on the provided service_id: access_levels pairs.

        Args:
            services_access (dict): Dictionary with service_id as key and access_levels as value.

        Returns:
            None
        """
        # Extracting service_ids directly from the keys as they are integers
        service_ids = [k for k, v in services_access.items()]

        Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        with Session() as session:
            # Fetching all services that need to be updated
            services = session.query(cls).filter(cls.service_id.in_(service_ids)).all()

            # If no services are found, you might want to log this or raise an exception
            if not services:
                print("No services found for provided IDs")

            # Updating each service's access_levels
            for service in services:
                # Check if the service_id is present in services_access dict before updating the access_levels
                if service.service_id in services_access:
                    service.access_levels = services_access[service.service_id]

            # Committing the changes to the database
            session.commit()

    @classmethod
    def enable_disable_column(cls, columns_status):
        """
        Enable or disable columns in services based on the provided columns status.

        Args:
            columns_status (dict): Dictionary with service_id as key and another dictionary as value, which contains column names and their respective status.

        Returns:
            None
        """
        # Extracting service_ids directly from the keys
        service_ids = [k for k, v in columns_status.items()]

        Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        with Session() as session:
            # Fetching all services that need to be updated
            services = session.query(cls).filter(cls.service_id.in_(service_ids)).all()

            # If no services are found, you might want to log this or raise an exception
            if not services:
                print("No services found for provided IDs")

            # Updating each service's table_columns
            for service in services:
                # Check if the service_id is present in columns_status dict before updating the table_columns
                if service.service_id in columns_status:
                    # Updating the respective columns' status
                    for column_name, status in columns_status[service.service_id].items():
                        # Check if the column_name exists in service's table_columns
                        if column_name in service.table_columns:
                            service.table_columns[column_name] = status
                        else:
                            print(f"Column {column_name} does not exist in service {service.service_id}")

            # Committing the changes to the database
            session.commit()